#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----
#
# Certain portions of the contents of this file are derived from TPC-DS version 3.2.0
# (retrieved from www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
# Such portions are subject to copyrights held by Transaction Processing Performance Council ("TPC")
# and licensed under the TPC EULA (a copy of which accompanies this file as "TPC EULA" and is also
# available at http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) (the "TPC EULA").
#
# You may not use this file except in compliance with the TPC EULA.
# DISCLAIMER: Portions of this file is derived from the TPC-DS Benchmark and as such any results
# obtained using this file are not comparable to published TPC-DS Benchmark results, as the results
# obtained from using this file do not comply with the TPC-DS Benchmark.
#

"""
Spark-based TPC-DS data generation — replaces the Hadoop MapReduce approach.

Distributes dsdgen across Spark executors, writes output to any Hadoop-compatible
filesystem (HDFS, S3, GCS, ABFS, local). Works with any Spark cluster manager
(K8s, YARN, Standalone, local).

Prerequisites:
    1. Build tpcds-gen (cd tpcds-gen && make)
    2. The build produces target/lib/dsdgen.jar which is a jar archive containing
       the tools/ directory (dsdgen binary + *.dst files).

Usage:
    spark-submit [--master k8s://... | yarn | ...] \\
        --archives tpcds-gen/target/lib/dsdgen.jar#dsdgen \\
        nds_gen_data_spark.py \\
        <scale> <parallel> <output_dir> [options]

    The --archives flag distributes and extracts the dsdgen toolset to every executor.

Example (K8s):
    spark-submit --master k8s://https://<api-server> \\
        --deploy-mode cluster \\
        --conf spark.kubernetes.container.image=<image-with-spark> \\
        --archives tpcds-gen/target/lib/dsdgen.jar#dsdgen \\
        nds_gen_data_spark.py 100 100 hdfs:///data/raw_sf100 --overwrite

Example (YARN):
    spark-submit --master yarn \\
        --archives tpcds-gen/target/lib/dsdgen.jar#dsdgen \\
        nds_gen_data_spark.py 1000 200 hdfs:///data/raw_sf1000

Example (local testing):
    spark-submit --master 'local[4]' \\
        --archives tpcds-gen/target/lib/dsdgen.jar#dsdgen \\
        nds_gen_data_spark.py 1 2 /tmp/nds_test_data --overwrite
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

from pyspark.sql import SparkSession

# All source (qualification/power-run) table names in TPC-DS
SOURCE_TABLE_NAMES = [
    'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
    'customer', 'customer_address', 'customer_demographics', 'date_dim',
    'dbgen_version', 'household_demographics', 'income_band', 'inventory',
    'item', 'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
    'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
    'web_sales', 'web_site',
]

# Maintenance (data-update) table names
MAINTENANCE_TABLE_NAMES = [
    's_catalog_order', 's_catalog_order_lineitem', 's_catalog_returns',
    's_inventory', 's_purchase', 's_purchase_lineitem', 's_store_returns',
    's_web_order', 's_web_order_lineitem', 's_web_returns',
    'delete', 'inventory_delete',
]


def run_dsdgen_and_read(child_index, scale, parallel, update=None):
    """Execute dsdgen for one child partition, yield (table_name, line) pairs.

    This function runs inside a Spark executor task. The dsdgen binary and its
    auxiliary files (*.dst) are expected under SparkFiles root, extracted from
    the archive passed via --archives dsdgen.jar#dsdgen.

    Each generated .dat file is read line-by-line and yielded as (table_name, line),
    which Spark then writes to the target filesystem partitioned by table_name.
    Memory usage is bounded: only one line is held in memory at a time.
    """
    from pyspark import SparkFiles

    # Locate dsdgen binary from the extracted archive.
    # The archive (dsdgen.tar.gz or dsdgen.jar) contains a tools/ directory.
    # With --archives '<archive>#dsdgen', Spark extracts contents under a
    # directory named 'dsdgen' in SparkFiles root.
    # Note: SparkFiles.getRootDirectory() may return a relative path (e.g. ".")
    # in K8s mode, so we must resolve to absolute paths to avoid issues when
    # subprocess.run() changes cwd before resolving the executable path.
    archive_root = os.path.abspath(SparkFiles.getRootDirectory())
    tools_dir = os.path.join(archive_root, "dsdgen", "tools")
    dsdgen_bin = os.path.join(tools_dir, "dsdgen")

    if not os.path.isfile(dsdgen_bin):
        # Provide detailed debug info for troubleshooting archive extraction
        import glob
        dsdgen_dir = os.path.join(archive_root, "dsdgen")
        debug_info = (
            f"archive_root={archive_root}, "
            f"dsdgen_dir exists={os.path.exists(dsdgen_dir)}, "
            f"dsdgen_dir isdir={os.path.isdir(dsdgen_dir)}"
        )
        if os.path.isdir(dsdgen_dir):
            contents = glob.glob(os.path.join(dsdgen_dir, "**"), recursive=True)[:30]
            debug_info += f", contents={contents}"
        elif os.path.isdir(archive_root):
            contents = glob.glob(os.path.join(archive_root, "**"), recursive=True)[:30]
            debug_info += f", root_contents={contents}"
        raise FileNotFoundError(
            f"dsdgen binary not found at {dsdgen_bin}. {debug_info}. "
            "Make sure --archives <archive>#dsdgen is set."
        )

    # Ensure the binary is executable (archive extraction may lose permissions)
    if not os.access(dsdgen_bin, os.X_OK):
        os.chmod(dsdgen_bin, 0o755)

    # Temp directory for dsdgen output; each task gets its own
    work_dir = tempfile.mkdtemp(prefix=f"dsdgen_c{child_index}_")

    try:
        cmd = [
            dsdgen_bin,
            "-dir", work_dir,
            "-force", "Y",
            "-scale", str(scale),
            "-parallel", str(parallel),
            "-child", str(child_index),
        ]
        if update is not None:
            cmd += ["-update", str(update)]

        proc = subprocess.run(cmd, cwd=tools_dir, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(
                f"dsdgen failed for child {child_index} (exit {proc.returncode}): "
                f"{proc.stderr}"
            )

        # Read .dat files and yield (table_name, line) pairs
        regular_suffix = f"_{child_index}_{parallel}.dat"

        for fname in sorted(os.listdir(work_dir)):
            filepath = os.path.join(work_dir, fname)
            if not os.path.isfile(filepath):
                continue

            table_name = None

            if fname.endswith(regular_suffix):
                # Regular table: call_center_1_100.dat → table "call_center"
                table_name = fname[: -len(regular_suffix)]
            elif fname.endswith(".dat"):
                # Delete / inventory_delete tables have different naming:
                # delete_<update>.dat, inventory_delete_<update>.dat
                for special in ("inventory_delete", "delete"):
                    if fname.startswith(special):
                        table_name = special
                        break

            if table_name is None:
                continue

            with open(filepath, "r") as f:
                for line in f:
                    stripped = line.rstrip("\n\r")
                    if stripped:
                        yield (table_name, stripped)

            os.remove(filepath)
    finally:
        # Best-effort cleanup of temp directory (may contain leftover files)
        shutil.rmtree(work_dir, ignore_errors=True)


def rename_partition_dirs(spark, output_dir, table_names):
    """Rename Hive-style 'table_name=xxx' directories to plain 'xxx'.

    Spark's partitionBy writes to directories like output_dir/table_name=call_center/.
    The rest of the NDS pipeline expects output_dir/call_center/. This function
    performs fast HDFS-level renames (metadata only, no data copy) via the Hadoop
    FileSystem Java API on the driver.
    """
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path

    base_path = Path(output_dir)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(base_path.toUri(), hadoop_conf)

    for table in table_names:
        hive_dir = Path(output_dir, f"table_name={table}")
        target_dir = Path(output_dir, table)

        if not fs.exists(hive_dir):
            continue

        if fs.exists(target_dir):
            # Merge into existing directory (e.g., incremental range generation)
            statuses = fs.listStatus(hive_dir)
            for status in statuses:
                src_path = status.getPath()
                # Skip _SUCCESS / hidden files
                if src_path.getName().startswith("_"):
                    continue
                dst_path = Path(target_dir, src_path.getName())
                fs.rename(src_path, dst_path)
            fs.delete(hive_dir, True)
        else:
            fs.rename(hive_dir, target_dir)

    # Clean up Hive-style _SUCCESS at root if present
    success_file = Path(output_dir, "_SUCCESS")
    if fs.exists(success_file):
        fs.delete(success_file, False)


def main():
    parser = argparse.ArgumentParser(
        description="Spark-based TPC-DS data generation (replaces MapReduce)."
    )
    parser.add_argument("scale", type=int,
                        help="Data scale factor in GB.")
    parser.add_argument("parallel", type=int,
                        help="Number of parallel dsdgen children (must be >= 2).")
    parser.add_argument("output_dir",
                        help="Output directory (hdfs://..., s3a://..., gs://..., or local path).")
    parser.add_argument("--range",
                        help='Child range "start,end" (inclusive). '
                             'Default: generate all children 1..parallel. '
                             'Useful for splitting work across multiple spark-submit invocations.')
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing output directory.")
    parser.add_argument("--update", type=int, default=None,
                        help="Generate update/maintenance dataset <n>.")
    parser.add_argument("--num_executors", type=int, default=None,
                        help="Hint for number of Spark partitions. "
                             "Default: one partition per child (optimal parallelism).")
    args = parser.parse_args()

    if args.parallel < 2:
        print("ERROR: parallel must be >= 2", file=sys.stderr)
        sys.exit(1)

    range_start = 1
    range_end = args.parallel
    if args.range:
        parts = args.range.split(",")
        if len(parts) != 2:
            print("ERROR: --range must be 'start,end'", file=sys.stderr)
            sys.exit(1)
        range_start, range_end = int(parts[0]), int(parts[1])
        if range_start < 1 or range_end > args.parallel or range_start > range_end:
            print("ERROR: range must satisfy 1 <= start <= end <= parallel", file=sys.stderr)
            sys.exit(1)

    children = list(range(range_start, range_end + 1))
    num_children = len(children)
    num_partitions = args.num_executors if args.num_executors else num_children
    table_names = MAINTENANCE_TABLE_NAMES if args.update else SOURCE_TABLE_NAMES

    # Capture args for closure (avoid serializing argparse Namespace)
    scale = args.scale
    parallel = args.parallel
    update = args.update

    spark = SparkSession.builder \
        .appName(f"NDS_DataGen_sf{scale}_p{parallel}") \
        .getOrCreate()
    sc = spark.sparkContext

    print(f"=== NDS Spark Data Generation ===")
    print(f"  Scale:     {scale} GB")
    print(f"  Parallel:  {parallel}")
    print(f"  Range:     {range_start}..{range_end} ({num_children} children)")
    print(f"  Output:    {args.output_dir}")
    print(f"  Overwrite: {args.overwrite}")
    print(f"  Update:    {update}")
    print(f"  Partitions:{num_partitions}")

    # Create RDD: one element per child index.
    # Each Spark task runs dsdgen for its child, reads output line by line,
    # and yields (table_name, line) pairs — no data accumulates in memory.
    children_rdd = sc.parallelize(children, numSlices=num_partitions)

    all_data_rdd = children_rdd.flatMap(
        lambda child: run_dsdgen_and_read(child, scale, parallel, update)
    )

    # Convert to DataFrame: [table_name: string, value: string]
    # partitionBy("table_name") writes separate directories per table WITHOUT shuffle —
    # each task independently splits its output into per-table files.
    df = spark.createDataFrame(all_data_rdd, ["table_name", "value"])

    # Determine write mode:
    #   --range   → append (incremental generation across multiple spark-submit runs)
    #   --overwrite → overwrite (fresh start, wipe existing data)
    #   default   → errorifexists (fail if output already exists)
    if args.range:
        write_mode = "append"
    elif args.overwrite:
        write_mode = "overwrite"
    else:
        write_mode = "errorifexists"
    df.write.partitionBy("table_name").mode(write_mode).text(args.output_dir)

    # Rename Hive-style "table_name=xxx" dirs to plain "xxx" for NDS pipeline compat
    rename_partition_dirs(spark, args.output_dir, table_names)

    print(f"=== Data generation complete: {args.output_dir} ===")
    spark.stop()


if __name__ == "__main__":
    main()
