# NDS v2.0 Automation
## Disclaimer

NDS is derived from the TPC-DS Benchmarks and as such any results obtained using NDS are not
comparable to published TPC-DS Benchmark results, as the results obtained from using NDS do not
comply with the TPC-DS Benchmarks.

## License

NDS is licensed under Apache License, Version 2.0.

Additionally, certain files in NDS are licensed subject to the accompanying [TPC EULA](TPC%20EULA.txt) (also 
available at http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).  Files subject to the TPC 
EULA are identified as such within the files.

You may not use NDS except in compliance with the Apache License, Version 2.0 and the TPC EULA.


## prerequisites:

1. python > 3.6
2. Necessary libraries 
    ```
    sudo apt install openjdk-8-jdk-headless gcc make flex bison byacc maven
    pip3 install pyspark
    ```
3. TPC-DS Tools

    User must download TPC-DS Tools from [official TPC website](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp). The tool will be downloaded as a zip package with a random guid string prefix.
    After unzipping it, a folder called `DSGen-software-code-3.2.0rc1` will be seen.

    User must set a system environment variable `TPCDS_HOME` pointing to this directory. e.g.
    ```
    export TPCDS_HOME=/PATH/TO/YOUR/DSGen-software-code-3.2.0rc1
    ```
    This variable will help find the TPC-DS Tool when building essential component for this repository.

## Data Generation


### build the jar for data generation:
```
cd tpcds-gen
make
```
Then two jars will be built at:
```
./target/tpcds-gen-1.0-SNAPSHOT.jar
./target/lib/dsdgen.jar
```

### Generate data

#### For HDFS

Note: please make sure you have `Hadoop binary` locally.

Checkout to the parent folder of the repo.
(We assume the working directory is always the parent folder in the following sections)

```
python nds.py \
--generate data \
--type hdfs \
--data-dir /PATH_FOR_DATA \
--scale 100 \
--parallel 100
```

Please note: HDFS data generation doesn't allow scale=1 or parallel=1.
#### For local FS
```
python nds.py \
--generate data \
--type local \
--data-dir /PATH_FOR_DATA \
--scale 100 \
--parallel 100
```

### Convert CSV to Parquet

The NDS python script will submit a Spark job to finish the data conversion. User should put necessary Spark configs into pre-defined template file.

Sample command to convert the data:
```
python nds.py \
--generate convert \
--spark-submit-template convert_submit.template \
--input-prefix hdfs:///data/nds_raw/ \
--output-prefix hdfs:///data/nds_parquet/
```

Note: the `/` at the end of `input-prefix` path is required.

We provide two basic templates for GPU run(convert_submit_gpu.template) and CPU run(convert_submit_cpu.template).
To enable GPU run, user need to download two jars in advance to use spark-rapids plugin.

- cuDF jar: https://repo1.maven.org/maven2/ai/rapids/cudf/22.02.0/cudf-22.02.0.jar
- spark-rapids jar: https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.02.0/rapids-4-spark_2.12-22.02.0.jar

After that, please set environment variable `CUDF_JAR` and `SPARK_RAPIDS_PLUGIN_JAR` to the path where the jars are downloaded to in spark submit templates.

### Data partitioning

when converting CSV to Parquet data, the script will add data partitioning to some tables:

| Table              | Partition Column    |
| -----------        | -----------         |
| catalog_sales      | cs_sold_date_sk     |
| catalog_returns    | cr_returned_date_sk |
| inventory          | inv_date_sk         |
| store_sales        | ss_sold_date_sk     |
| store_returns      | sr_returned_date_sk |
| web_sales          | ws_sold_date_sk     |
| web_returns        | wr_returned_date_sk |

## Query Generation
The [templates.patch](./tpcds-gen/patches/templates.patch) that contains necessary modifications to make NDS queries runnable in Spark will be applied automatically in the build step. The final query templates will be in folder `$TPCDS_HOME/query_templates` after the build process.

we applied the following changes to original templates released in TPC-DS v3.2.0:

- add `interval` keyword before all `date interval add` mark `+` for syntax compatibility in Spark SQL.

- convert `"` mark to `` ` `` mark for syntax compatibility in Spark SQL.


### Generate Specific Query

Sample command to generate query1 from template for NDS:
```
python nds.py \
--generate query \
--template query1.tpl \
--template-dir $TPCDS_HOME/query_templates \
--scale 100 \
--query-output-dir ./nds_queries

```

### Generate Query Streams

Sample command to generate query streams used for Power Run and Throughput Run.
```
python nds.py \
--generate streams \
--template-dir $TPCDS_HOME/query_templates \
--scale 100 \
--query-output-dir ./nds_query_streams \
--streams 10
```

## Benchmark Runner

### Power Run

_After_ user generates query streams, Power Run can be executed using one of the streams.

Sample command for Power Run:
```
python nds.py \
--run power \
--query-stream $TPCDS_HOME/query_templates/query_0.sql \
--input-prefix hdfs:///data/NDS_parquet/ \
--run-log test.log \
--spark-submit-template power_run_gpu.template \
--csv-output time.csv \
```

When it's finished, user will see parsed logs from terminal like:
```
......
......
====== Run query4 ======
Time taken: 25532 ms
====== Run query94 ======
Time taken: 886 ms
====== Run query20 ======
Time taken: 1237 ms
====== Run query14a ======
Time taken: 11951 ms

====== Total time : 325939 ms ======

```

To simplify the performance analysis process, the script will create a CSV file to save query and corresponding execution time.
The file path is defined by `--csv-output` argument.

### Throughput Run
Throughput Run simulates the scenario that multiple query sessions are running simultaneously in Spark. Different to Power Run, user needs to provide multiple query streams as input for `--query-stream` argument with `,` as seperator. Also the run log will be saved for each query stream independently with index number as naming suffix like _test.log_query_1_, _test.log_query2_ etc. and _time.csv_query_1_, _time.csv_query2_ etc.

When providing `spark-submit-template` to Throughput Run, please do consider the computing resources in your environment to make sure all Spark job can get necessary resources to run at the same time, otherwise some query application may be in _WAITING_ status(which can be observed from Spark UI or Yarn Resource Manager UI) until enough resources are released.

Sample command for Throughput Run:

```
python nds.py \
--run power \
--query-stream $TPCDS_HOME/query_templates/query_0.sql,$TPCDS_HOME/query_templates/query_1.sql \
--input-prefix hdfs:///data/NDS_parquet/ \
--run-log test.log \
--spark-submit-template power_run_gpu.template \
--csv-output time.csv \
```


### NDS2.0 is using source code from TPC-DS Tool V3.2.0