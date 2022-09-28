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

1. python >= 3.6
2. Necessary libraries 
    ```
    sudo locale-gen en_US.UTF-8
    sudo apt install openjdk-8-jdk-headless gcc make flex bison byacc maven
    ```
3. TPC-DS Tools

    User must download TPC-DS Tools from [official TPC website](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp). The tool will be downloaded as a zip package with a random guid string prefix.
    After unzipping it, a folder called `DSGen-software-code-3.2.0rc1` will be seen.

    User must set a system environment variable `TPCDS_HOME` pointing to this directory. e.g.
    ```
    export TPCDS_HOME=/PATH/TO/YOUR/DSGen-software-code-3.2.0rc1
    ```
    This variable will help find the TPC-DS Tool when building essential component for this repository.

## Use spark-submit-template with template
To help user run NDS, we provide a template to define the main Spark configs for spark-submit command.
User can use different templates to run NDS with different configurations for different environment.
We create [spark-submit-template](./spark-submit-template), which accepts a template file and
submit the Spark job with the configs defined in the template file.

Example command to submit via `spark-submit-template` utility:
```
./spark-submit-template convert_submit_cpu.template \
nds_transcode.py  raw_sf3k  parquet_sf3k report.txt
```

We give 3 types of template files used in different steps of NDS:
1. convert_submit_*.template for converting the data by using nds_transcode.py
2. maintenance_*.template for data maintenance by using nds_maintenance.py
3. power_run_*.template for power run by using nds_power.py

We predefine different template files for different environment.
For example, we provide below template files to run nds_transcode.py for different environment:
* `convert_submit_cpu.template` is for Spark CPU cluster
* `convert_submit_cpu_delta.template` is for Spark CPU cluster with DeltaLake
* `convert_submit_cpu_iceberg.template` is for Spark CPU cluster with Iceberg
* `convert_submit_gpu.template` is for Spark GPU cluster

You need to choose one as your template file and modify it to fit your environment.
We define a [base.template](./base.template) to help you define some basic variables for your envionment.
And all the other templates will source `base.template` to get the basic variables.
When you hope to run multiple steps of NDS, you just need to modify `base.template` to fit for your cluster.


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

How to generate data to local or HDFS:
```
$ python nds_gen_data.py -h
usage: nds_gen_data.py [-h] [--range RANGE] [--overwrite_output] {local,hdfs} scale parallel data_dir
positional arguments:
  {local,hdfs}        file system to save the generated data.
  scale               volume of data to generate in GB.
  parallel            build data in <parallel_value> separate chunks
  data_dir            generate data in directory.

optional arguments:
  -h, --help          show this help message and exit
  --range RANGE       Used for incremental data generation, meaning which part of childchunks are
                      generated in one run. Format: "start,end", both are inclusive. e.g. "1,100". Note:
                      the child range must be within the "parallel", "--parallel 100 --range 100,200" is
                      illegal.
  --overwrite_output  overwrite if there has already existing data in the path provided.
  --replication REPLICATION
                      the number of replication factor when generating data to HDFS. if not set, the Hadoop job will use the setting in the Hadoop cluster.
  --update UPDATE     generate update dataset <n>. <n> is identical to the number of streams used in the Throughput Tests of the benchmark
```

Example command:
```
python nds_gen_data.py hdfs 100 100 /data/raw_sf100 --overwrite_output
```


### Convert CSV to Parquet or Other data sources

To do the data conversion, the `nds_transcode.py` need to be submitted as a Spark job. User can leverage
the [spark-submit-template](./spark-submit-template) utility to simplify the submission.
The utility requires a pre-defined [template file](./convert_submit_gpu.template) where user needs to put
necessary Spark configurations. Either user can submit the `nds_transcode.py` directly to spark with
arbitrary Spark parameters.

Parquet, Orc, Avro, JSON and Iceberg are supported for output data format at present with CPU. For GPU conversion,
only Parquet and Orc are supported.

Note: when exporting data from CSV to Iceberg, user needs to set necessary configs for Iceberg in submit template.
e.g. [convert_submit_cpu_iceberg.template](./convert_submit_cpu_iceberg.template)

User can also specify `--tables` to convert specific table or tables. See argument details below.

if `--floats` is specified in the command, DoubleType will be used to replace DecimalType data in Parquet files,
otherwise DecimalType will be saved.

#### NOTE: DeltaLake tables

To convert CSV to DeltaLake [managed tables](https://docs.databricks.com/lakehouse/data-objects.html#what-is-a-managed-table),
user needs to leverage a hive metastore service. For example, on Dataproc, you can use Dataproc Metastore service.
When [creating a Dataproc Metastore service](https://cloud.google.com/dataproc-metastore/docs/create-service-cluster),
user needs to specify the `hive.metastore.warehouse.dir` to your desired gs bucket at section `Metastore config overrides`
as the DeltaLake warehouse directory. e.g. `hive.metastore.warehouse.dir=gs://YOUR_BUCKET/warehouse`.
This action is required when set `--output_format` to `delta` when transcoding. Note, the `output_prefix`
will not take effect in this situation.
Don't forget to `export` Metastore content that contains database and table metadata to a gs bucket
when you are about to shutdown the Metastore service.

For [unmanaged tables](https://docs.databricks.com/lakehouse/data-objects.html#what-is-an-unmanaged-table),
user doesn't need to create the Metastore service,  appending `--delta_unmanaged` to arguments will be enough.


Arguments for `nds_transcode.py`:
```
python nds_transcode.py -h
usage: nds_transcode.py [-h] [--output_mode {overwrite,append,ignore,error,errorifexists}] [--output_format {parquet,orc,avro,json,iceberg,delta}] [--tables TABLES] [--log_level LOG_LEVEL] [--floats] [--update] [--iceberg_write_format {parquet,orc,avro}] [--compression COMPRESSION] [--delta_unmanaged]
                        input_prefix output_prefix report_file

positional arguments:
  input_prefix          text to prepend to every input file path (e.g., "hdfs:///ds-generated-data"; the
                        default is empty)
  output_prefix         text to prepend to every output file (e.g., "hdfs:///ds-parquet"; the default is empty). If output_format is "iceberg", this argument will be regarded as the value of property "spark.sql.catalog.spark_catalog.warehouse". Only default Spark catalog session
                        name "spark_catalog" is supported now, customized catalog is not yet supported.
  report_file           location to store a performance report(local)

optional arguments:
  -h, --help            show this help message and exit
  --output_mode {overwrite,append,ignore,error,errorifexists}
                        save modes as defined by https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes.default value is errorifexists, which is the Spark default behavior.
  --output_format {parquet,orc,avro,json,iceberg,delta}
                        output data format when converting CSV data sources.
  --tables TABLES       specify table names by a comma separated string. e.g. 'catalog_page,catalog_sales'.
  --log_level LOG_LEVEL
                        set log level for Spark driver log. Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN(default: INFO)
  --floats              replace DecimalType with DoubleType when saving parquet files. If not specified, decimal data will be saved.
  --update              transcode the source data or update data
  --iceberg_write_format {parquet,orc,avro}
                        File format for the Iceberg table; parquet, avro, or orc
  --compression COMPRESSION
                        Compression codec to use when saving data. See https://iceberg.apache.org/docs/latest/configuration/#write-properties for supported codecs in Iceberg. See
                        https://spark.apache.org/docs/latest/sql-data-sources.html for supported codecs for Spark built-in formats. When not specified, the default for the requested output format will be used.
  --delta_unmanaged     Use unmanaged tables for DeltaLake. This is useful for testing DeltaLake without leveraging a
                        Metastore service
```

Example command to submit via `spark-submit-template` utility:
```
./spark-submit-template convert_submit_gpu.template \
nds_transcode.py  raw_sf3k  parquet_sf3k report.txt
```

User can also use `spark-submit` to submit `nds_transcode.py` directly.

We provide two basic templates for GPU run(convert_submit_gpu.template) and CPU run(convert_submit_cpu.template).
To enable GPU run, user needs to download the following jar.

- spark-rapids jar: https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.06.0/rapids-4-spark_2.12-22.06.0.jar

After that, please set environment variable `SPARK_RAPIDS_PLUGIN_JAR` to the path where the jars are
downloaded to in spark submit templates.

### Data partitioning

When converting CSV to Parquet data, the script will add data partitioning to some tables:

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


### Generate Specific Query or Query Streams

```
usage: nds_gen_query_stream.py [-h] (--template TEMPLATE | --streams STREAMS)
                               template_dir scale output_dir

positional arguments:
  template_dir         directory to find query templates and dialect file.
  scale                assume a database of this scale factor.
  output_dir           generate query in directory.

optional arguments:
  -h, --help           show this help message and exit
  --template TEMPLATE  build queries from this template
  --streams STREAMS    generate how many query streams. If not specified, only one query will be produced.

```

Example command to generate one query using query1.tpl:
```
python nds_gen_query_stream.py $TPCDS_HOME/query_templates 3000 ./query_1 --template query1.tpl
```

Example command to generate 10 query streams each one of which contains all NDS queries but in 
different order:
```
python nds_gen_query_stream.py $TPCDS_HOME/query_templates 3000 ./query_streams --streams 10
```

## Benchmark Runner

### Power Run

_After_ user generates query streams, Power Run can be executed using one of them by submitting `nds_power.py` to Spark.

Arguments supported by `nds_power.py`:
```
usage: nds_power.py [-h] [--input_format {parquet,orc,avro,csv,json,iceberg,delta}] [--output_prefix OUTPUT_PREFIX] [--output_format OUTPUT_FORMAT] [--property_file PROPERTY_FILE] [--floats] [--json_summary_folder JSON_SUMMARY_FOLDER] [--delta_unmanaged] input_prefix query_stream_file time_log

positional arguments:
  input_prefix          text to prepend to every input file path (e.g., "hdfs:///ds-generated-data"). If input_format is "iceberg", this argument will be regarded as the value of property "spark.sql.catalog.spark_catalog.warehouse". Only default Spark catalog session name
                        "spark_catalog" is supported now, customized catalog is not yet supported. Note if this points to a Delta Lake table, the path must be absolute. Issue: https://github.com/delta-io/delta/issues/555
  query_stream_file     query stream file that contains NDS queries in specific order
  time_log              path to execution time log, only support local path.

optional arguments:
  -h, --help            show this help message and exit
  --input_format {parquet,orc,avro,csv,json,iceberg,delta}
                        type for input data source, e.g. parquet, orc, json, csv or iceberg, delta. Certain types are not fully supported by GPU reading, please refer to https://github.com/NVIDIA/spark-rapids/blob/branch-22.08/docs/compatibility.md for more details.
  --output_prefix OUTPUT_PREFIX
                        text to prepend to every output file (e.g., "hdfs:///ds-parquet")
  --output_format OUTPUT_FORMAT
                        type of query output
  --property_file PROPERTY_FILE
                        property file for Spark configuration.
  --floats              When loading Text files like json and csv, schemas are required to determine if certain parts of the data are read as decimal type or not. If specified, float data will be used.
  --json_summary_folder JSON_SUMMARY_FOLDER
                        Empty folder/path (will create if not exist) to save JSON summary file for each query.
  --delta_unmanaged     Use unmanaged tables for DeltaLake. This is useful for testing DeltaLake without leveraging a
                        Metastore service
```

Example command to submit nds_power.py by spark-submit-template utility:
```
./spark-submit-template power_run_gpu.template \
nds_power.py \
parquet_sf3k \
./nds_query_streams/query_0.sql \
time.csv \
--property_file properties/aqe-on.properties
```

User can also use `spark-submit` to submit `nds_power.py` directly.

To simplify the performance analysis process, the script will create a local CSV file to save query(including TempView creation) and corresponding execution time. Note: please use `client` mode(set in your `power_run_gpu.template` file) when running in Yarn distributed environment to make sure the time log is saved correctly in your local path.

Note the template file must follow the `spark-submit-template` utility as the _first_ argument.
All Spark configuration words (such as `--conf` and corresponding `k=v` values)  are quoted by
double quotes in the template file. Please follow the format in [power_run_gpu.template](./power_run_gpu.template).

User can define the `properties` file like [aqe-on.properties](./properties/aqe-on.properties). The properties will be passed to the submitted Spark job along with the configurations defined in the template file. User can define some common properties in the template file and put some other properties that usually varies in the property file.


The command above will use `collect()` action to trigger Spark job for each query. It is also supported to save query output to some place for further verification. User can also specify output format e.g. csv, parquet or orc:
```
./spark-submit-template power_run_gpu.template \
nds_power.py \
parquet_sf3k \
./nds_query_streams/query_0.sql \
time.csv \
--output_prefix /data/query_output \
--output_format parquet
```

Note there's a customized Spark listener used to track the Spark task status e.g. success or failed 
or success with retry. The results will be recorded at the json summary files when all jobs are
finished. This is often used for test or query monitoring purpose.

### Throughput Run
Throughput Run simulates the scenario that multiple query sessions are running simultaneously in
Spark.

We provide an executable bash utility `nds-throughput` to do Throughput Run.

Example command for Throughput Run that runs _2_ Power Run in parallel with stream file _query_1.sql_
and _query_2.sql_ and produces csv log for execution time _time_1.csv_ and _time_2.csv_.

```
./nds-throughput 1,2 \
./spark-submit-template power_run_gpu.template \
nds_power.py \
parquet_sf3k \
./nds_query_streams/query_'{}'.sql \
time_'{}'.csv
```

When providing `spark-submit-template` to Throughput Run, please do consider the computing resources
in your environment to make sure all Spark job can get necessary resources to run at the same time,
otherwise some query application may be in _WAITING_ status(which can be observed from Spark UI or 
Yarn Resource Manager UI) until enough resources are released.

### Data Maintenance
Data Maintenance performance data update over existed dataset including data INSERT and DELETE. The
update operations cannot be done atomically on raw Parquet/Orc files, so we use
[Iceberg](https://iceberg.apache.org/) as dataset metadata manager to overcome the issue.

Enabling Iceberg requires additional configuration. Please refer to [Iceberg Spark](https://iceberg.apache.org/docs/latest/getting-started/)
for details. We also provide a Spark submit template with necessary Iceberg configs: [maintenance.template](./maintenance.template)

The data maintenance queries are in [data_maintenance](./data_maintenance) folder. `DF_*.sql` are
DELETE queries while `LF_*.sql` are INSERT queries.

Note: The Delete functions in Data Maintenance cannot run successfully in Spark 3.2.0 and 3.2.1 due 
to a known Spark [issue](https://issues.apache.org/jira/browse/SPARK-39454). User can run it in Spark 3.2.2
or later. More details including work-around for version 3.2.0 and 3.2.1 could be found in this 
[link](https://github.com/NVIDIA/spark-rapids-benchmarks/pull/9#issuecomment-1141956487)

Arguments supported for data maintenance:
```
usage: nds_maintenance.py [-h] [--maintenance_queries MAINTENANCE_QUERIES] [--property_file PROPERTY_FILE] [--json_summary_folder JSON_SUMMARY_FOLDER] [--warehouse_type {iceberg,delta}] [--delta_unmanaged] warehouse_path refresh_data_path maintenance_queries_folder time_log

positional arguments:
  warehouse_path        warehouse path for Data Maintenance test.
  refresh_data_path     path to refresh data
  maintenance_queries_folder
                        folder contains all NDS Data Maintenance queries. If "--maintenance_queries"
                        is not set, all queries under the folder will beexecuted.
  time_log              path to execution time log in csv format, only support local path.

optional arguments:
  -h, --help            show this help message and exit
  --maintenance_queries MAINTENANCE_QUERIES
                        specify Data Maintenance query names by a comma seprated string. e.g. "LF_CR,LF_CS"
  --property_file PROPERTY_FILE
                        property file for Spark configuration.
  --json_summary_folder JSON_SUMMARY_FOLDER
                        Empty folder/path (will create if not exist) to save JSON summary file for each query.
  --warehouse_type {iceberg,delta}
                        Type of the warehouse used for Data Maintenance test.
  --delta_unmanaged     Use unmanaged tables for DeltaLake. This is useful for testing DeltaLake without leveraging a Metastore service.
```

An example command to run only _LF_CS_ and _DF_CS_ functions:
```
./spark-submit-template maintenance.template \
nds_maintenance.py \
update_data_sf3k \
./data_maintenance \
time.csv \
--maintenance_queries LF_CS,DF_CS \
--data_format orc
```

Note: to make the maintenance query compatible in Spark, we made the following changes:
1. change `CREATE VIEW` to `CREATE TEMP VIEW` in all INSERT queries due to [[SPARK-29630]](https://github.com/apache/spark/pull/26361)
2. change data type for column `sret_ticket_number` in table `s_store_returns` from `char(20)` to `bigint` due to [known issue](https://github.com/NVIDIA/spark-rapids-benchmarks/pull/9#issuecomment-1138379596)

## Data Validation
To validate query output between Power Runs with and without GPU, we provide [nds_validate.py](nds_validate.py)
to do the job.

Arguments supported by `nds_validate.py`:
```
usage: nds_validate.py [-h] [--input1_format INPUT1_FORMAT] [--input2_format INPUT2_FORMAT] [--max_errors MAX_ERRORS] [--epsilon EPSILON] [--ignore_ordering] [--use_iterator] [--floats] --json_summary_folder JSON_SUMMARY_FOLDER input1 input2 query_stream_file

positional arguments:
  input1                path of the first input data.
  input2                path of the second input data.
  query_stream_file     query stream file that contains NDS queries in specific order.

optional arguments:
  -h, --help            show this help message and exit
  --input1_format INPUT1_FORMAT
                        data source type for the first input data. e.g. parquet, orc. Default is: parquet.
  --input2_format INPUT2_FORMAT
                        data source type for the second input data. e.g. parquet, orc. Default is: parquet.
  --max_errors MAX_ERRORS
                        Maximum number of differences to report.
  --epsilon EPSILON     Allow for differences in precision when comparing floating point values.
                        Given 2 float numbers: 0.000001 and 0.000000, the diff of them is 0.000001 which is less than the epsilon 0.00001, so we regard this as acceptable and will not report a mismatch.
  --ignore_ordering     Sort the data collected from the DataFrames before comparing them.
  --use_iterator        When set, use `toLocalIterator` to load one partition at a time into driver memory, reducing.
                        memory usage at the cost of performance because processing will be single-threaded.
  --floats              whether the input data contains float data or decimal data. There're some known mismatch issues due to float point, we will do some special checks when the input data is float for some queries.
  --json_summary_folder JSON_SUMMARY_FOLDER
                        path of a folder that contains json summary file for each query.

```

Example command to compare output data of two queries:
```
python nds_validate.py \
query_output_cpu \
query_output_gpu \
./nds_query_streams/query_1.sql \
--ignore_ordering
```

## Whole Process NDS Benchmark
[nds_bench.py](./nds_bench.py) along with its yaml config file [bench.yml](./bench.yml) is the script
to run the whole process NDS benchmark to get final metrics.
User needs to fill in the config file to specify the parameters of the benchmark.
User can specify the `skip` field in the config file to skip certain part of the benchmarks.
Please note: each part of the benchmark will produce its report file for necessary metrics like total
execution time, start or end timestamp. The final metrics are calculated by those reports. Skipping
a part of the benchmark may cause metrics calculation failure in the end if there's no necessary reports
generated previously.

Example command to run the benchmark:
```
usage: nds_bench.py [-h] yaml_config

positional arguments:
  yaml_config  yaml config file for the benchmark
```
### NDS2.0 is using source code from TPC-DS Tool V3.2.0
