# Portable YARN resource cost

Spark elapsed time, task duration, YARN vcore-seconds, and Spark
executor-core-seconds answer different questions. None of them consistently
describes the fraction of worker nodes that YARN allocated to an application.
For example, `DefaultResourceCalculator` schedules by memory even when Spark
advertises several executor cores. Comparing raw vcore-seconds across clusters
can therefore undercount memory-heavy containers or compare unrelated YARN
accounting units.

This tool reconstructs container lifetimes and allocations from ResourceManager
logs, converts them into node-equivalent seconds, and optionally applies an
auditable hourly rate. Spark event logs select applications and provide names,
wall-clock duration, executor/container joins, and task-packing diagnostics;
they are not treated as the allocation ledger.

## Requirements

- Python 3.10 or newer; the accounting code has no third-party Python packages.
- Archived Spark rolling event logs.
- ResourceManager logs containing node registrations, allocations, terminal
  transitions, application summaries, and ResourceCalculator evidence.
- NodeManager logs are accepted only as incomplete fallback evidence.
- The provider CLI is needed only for remote discovery: AWS CLI for EMR,
  `gcloud` for Dataproc, or `hdfs` for on-premises HDFS paths.

## Accounting model

For a container lasting `ContainerSeconds` on a node:

```text
DefaultResourceCalculator:
  NodeShare = ContainerMemoryMB / NodeMemoryMB

DominantResourceCalculator:
  NodeShare = max(ContainerResource[r] / NodeResource[r]) for every r

NodeEquivalentSeconds = ContainerSeconds * NodeShare
```

## Python API

Install the subproject and its optional AWS dependency:

```bash
python3 -m pip install './yarn-resource-cost[aws]'
```

The application-scoped API accepts injected boto3 clients and returns resource
usage without imposing a pricing policy:

```python
import boto3

from yarn_resource_cost import (
    EmrApplicationUsageRequest,
    calculate_emr_application_usage,
)

session = boto3.Session(region_name="us-west-2")
usage = calculate_emr_application_usage(
    EmrApplicationUsageRequest(
        cluster_id="j-EXAMPLE",
        application_id="application_123_0001",
        event_log_uri="s3://example-bucket/spark-events/eventlog_v2_application_123_0001/",
        region="us-west-2",
    ),
    emr_client=session.client("emr"),
    s3_client=session.client("s3"),
)
print(usage.instance_seconds_by_type)
```

Incomplete archived logs return `complete=False` and indicate whether a later
retry can help. Authentication and transport errors propagate from boto3. The
caller decides whether and how to translate instance-seconds into currency.

Memory, vcores, `yarn.io/gpu`, and arbitrary numeric custom resources are
parsed generically. Heterogeneous node classes remain separate in structured
output and expressions such as:

```text
123.4 aws:ec2:r7a.24xlarge-seconds + 50.0 aws:ec2:g6.4xlarge-seconds
```

The calculator comes from archived scheduler evidence. There is intentionally
no calculator override. Built-in FairScheduler policies map to their actual
memory or dominant-resource calculators; ambiguous or conflicting policy
evidence is not guessed.

Final worker cost is emitted only for complete ledgers. Missing RM allocations,
terminal transitions, node capacities, application summaries, event-log
segments, node classifications, or price-catalog entries suppress final cost.

## Usage

Resource accounting is offline by default:

```bash
python3 yarn-resource-cost/yarn_resource_cost.py \
  --adapter emr \
  --event-log-root s3://example-bucket/run/spark-events/ \
  --pricing none
```

EMR live on-demand pricing includes EC2 and EMR worker components and records
the lookup result and timestamp:

```bash
python3 yarn-resource-cost/yarn_resource_cost.py \
  --adapter emr \
  --event-log-root s3://example-bucket/run/spark-events/ \
  --pricing live \
  --aws-profile example-profile \
  --aws-region us-west-2 \
  --output-json emr-cost.json
```

Dataproc accepts `gs://` Spark logs and exported RM/NM daemon logs. A node-class
map separates machine and accelerator shapes:

```bash
python3 yarn-resource-cost/yarn_resource_cost.py \
  --adapter dataproc \
  --event-log-root gs://example-bucket/spark-events/ \
  --yarn-log-root gs://example-bucket/cluster-daemon-logs/ \
  --node-class-map dataproc-node-classes.json \
  --pricing catalog \
  --price-catalog dataproc-prices.json
```

On-premises inputs can be directories, ZIP/tar archives, or HDFS paths:

```bash
python3 yarn-resource-cost/yarn_resource_cost.py \
  --adapter on-prem \
  --event-log-root /archive/spark-events \
  --yarn-log-root /archive/yarn-daemon-logs.tar.gz \
  --node-class-map node-classes.json \
  --pricing catalog \
  --price-catalog internal-rates.json \
  --output-csv application-cost.csv
```

Compare a baseline with one or more test reruns. Later test roots replace earlier
ones with the same comparison key:

```bash
python3 yarn-resource-cost/yarn_resource_cost.py \
  --adapter emr \
  --event-log-root s3://example-bucket/baseline/events/ \
  --test-event-log-root s3://example-bucket/test/events/ \
  --comparison-key regex \
  --comparison-key-regex 'job-(?P<key>[0-9]+)' \
  --pricing live \
  --sort-by cost-factor
```

For non-EMR comparisons, pair every `--test-event-log-root` with a
`--test-yarn-log-root` in the same order.

## Input schemas

Node classes are adapter-stable worker shapes, not hostnames:

```json
{
  "schema_version": 1,
  "nodes": {
    "worker-01.example.net": "onprem:gpu-a10-16c-128g"
  },
  "default_node_class": "onprem:cpu-32c-256g"
}
```

Pricing is deliberately separate from accounting:

```json
{
  "schema_version": 1,
  "currency": "USD",
  "effective_at": "2026-08-01T00:00:00Z",
  "source": "approved internal rate card",
  "rates": [
    {"node_class": "onprem:cpu-32c-256g", "hourly_rate": 4.25}
  ]
}
```

JSON output uses `schema_version: 1` and retains input roots, discovery source,
calculator evidence, node capacities, resource expressions, completeness,
warnings, pricing provenance, applications, and run summaries.

## Reproducible experiment capture

Archive these together for every benchmark run:

1. All Spark event-log segments through `SparkListenerApplicationEnd`.
2. All rolled ResourceManager and NodeManager daemon logs for the cluster.
3. `yarn-site.xml`, `resource-types.xml`, and the active
   `capacity-scheduler.xml` or `fair-scheduler.xml`.
4. Provider cluster descriptions and host-to-node-class mappings.
5. A frozen price catalog when results must be reproducible later.
6. A small manifest linking each event-log root to its YARN log root and node
   map. Never rely on a live cluster remaining available.

## Compatibility command

`calculate_yarn_job_cost.py` preserves the original EMR-oriented command and
output columns for existing integrations. New integrations should use
`yarn_resource_cost.py` and its provider-neutral versioned JSON output.

## License and contribution

The tool is licensed under Apache License 2.0 as part of this repository. See
the repository `LICENSE` and `CONTRIBUTING.md`. Generated standalone bundles
include both documents and a checksum manifest.
