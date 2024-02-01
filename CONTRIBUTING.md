# Contributing to Spark RAPIDS Benchmarks

Contributing to Spark RAPIDS Benchmarks fall into the following three categories.

1. To report a bug, request a new feature, or report a problem with
    documentation, please file an issue
    describing in detail the problem or new feature. The project team evaluates
    and triages issues, and schedules them for a release. If you believe the
    issue needs priority attention, please comment on the issue to notify the
    team.
2. To propose and implement a new feature, please file a new feature request
    issue. Describe the
    intended feature and discuss the design and implementation with the team and
    community. Once the team agrees that the plan looks good, go ahead and
    implement it using the [code contributions](#code-contributions) guide below.
3. To implement a feature or bug-fix for an existing outstanding issue, please
    follow the [code contributions](#code-contributions) guide below. If you
    need more context on a particular issue, please ask in a comment.

## Building From Source
[Nvidia Decision Support(NDS)](./nds/): 
        
- Please refer to [nds/README](./nds/README.md#prerequisites) for prerequisites and build instructions.

- Note: the build step aims to:
    1. Apply code and query template modifications to original TPC-DS toolkit to make it compatible to
Spark (see [patches](./nds/tpcds-gen/patches/))
    2. Use maven to build a maven project that is used to only generate data on HDFS.
(see [tpcds-gen/src](./nds/tpcds-gen/src/))

## Code contributions

### Source code layout

The repository contains the following parts:

[Nvidia Decision Support(NDS)](./nds):
- `cicd` contains an settings.xml file used for maven build tool
- `properties` property files that contains Spark configs used for submitting Spark jobs
- `pyspark_spy` a third party library used to add SparkListener via pyspark
- `tpcds-gen`
    - `patches` code changes and template modifications based on original TPC-DS tool
    - `src` Hadoop application code to generate data in HDFS
- `PysparkBenchReport.py` generate json summary report reflecting statistics for NDS run
- `check.py` utils to check build and validate input
- `nds_gen_data.py` generate data in local or hdfs
- `nds_gen_query_stream.py` generate query streams or specific query
- `nds_power.py` functionality to execute Power Run
- `nds_transcode.py` used to convert CSV data to Parquet
- `*.template` template file contains Spark configs to submit a Power Run
- `spark-submit-template` script to process template content

### Integrated Development Environment
For python scripts, VSCode or PyCharm are recommended but developers can choose arbitary IDE they prefer.
For java code in [tpcds-gen/src](./nds/tpcds-gen/src/), IntelliJ IDEA is recommended.
It will download necessary dependencies once it opens the folder as a maven project.

### Your first issue

1. Read the project's [README.md](./nds/README.md) to learn how to build the project and run scripts.
2. Find an issue to work on.

## Coding style
1. For Python [PEP8](https://www.python.org/dev/peps/pep-0008) is used to check the adherence to this style.
2. For Java [Oracle Java code conventions](http://www.oracle.com/technetwork/java/codeconvtoc-136057.html) is used to check the adherence to this style.

### Sign your work

We require that all contributors sign-off on their commits. This certifies that the contribution is
your original work, or you have rights to submit it under the same license, or a compatible license.

Any contribution which contains commits that are not signed off will not be accepted.

To sign off on a commit use the `--signoff` (or `-s`) option when committing your changes:

```shell
git commit -s -m "Add cool feature."
```

This will append the following to your commit message:

```
Signed-off-by: Your Name <your@email.com>
```

The sign-off is a simple line at the end of the explanation for the patch. Your signature certifies
that you wrote the patch or otherwise have the right to pass it on as an open-source patch. Use your
real name, no pseudonyms or anonymous contributions.  If you set your `user.name` and `user.email`
git configs, you can sign your commit automatically with `git commit -s`.


The signoff means you certify the below (from [developercertificate.org](https://developercertificate.org)):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

### Pre-commit hooks

We provide a basic config `.pre-commit-config.yaml` for [pre-commit](https://pre-commit.com/) to
automate some aspects of the development process. As a convenience you can enable automatic
copyright year updates by following the installation instructions on the
[pre-commit homepage](https://pre-commit.com/).

To this end, first install `pre-commit` itself using the method most suitable for your development
environment. Then you will need to run `pre-commit install` to enable it in your local git
repository. Using `--allow-missing-config` will make it easy to work with older branches
that do not have `.pre-commit-config.yaml`.

```bash
pre-commit install --allow-missing-config
```

and setting the environment variable:

```bash
export SPARK_RAPIDS_BENCHMARKS_AUTO_COPYRIGHTER=ON
```
The default value of `SPARK_RAPIDS_BENCHMARKS_AUTO_COPYRIGHTER` is `OFF`.

When automatic copyright updater is enabled and you modify a file with a prior
year in the copyright header it will be updated on `git commit` to the current year automatically.
However, this will abort the [commit process](https://github.com/pre-commit/pre-commit/issues/532)
with the following error message:
```
Update copyright year....................................................Failed
- hook id: auto-copyrighter
- duration: 0.01s
- files were modified by this hook
```
You can confirm that the update has actually happened by either inspecting its effect with
`git diff` first or simply re-executing `git commit` right away. The second time no file
modification should be triggered by the copyright year update hook and the commit should succeed.

There is a known issue for macOS users if they use the default version of `sed`. The copyright update
script may fail and generate an unexpected file named `source-file-E`. As a workaround, please
install GNU sed

```bash
brew install gnu-sed
# and add to PATH to make it as default sed for your shell
export PATH="/usr/local/opt/gnu-sed/libexec/gnubin:$PATH"
```

## Attribution
Portions adopted from https://github.com/NVIDIA/spark-rapids/blob/main/CONTRIBUTING.md
