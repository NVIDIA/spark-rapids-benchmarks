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


Note: please make sure you have `Hadoop binary` locally.

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
```

Example command:
```
python nds_gen_data.py hdfs 100 100 /data/raw_sf100 --overwrite_output
```


### Convert CSV to Parquet or Other data sources

To do the data conversion, the `nds_transcode.py` need to be submitted as a Spark job. User can leverage
the [spark-submit-template](./spark-submit-template) utilty to simpify the submission.
The utility requires a pre-defined [template file](./convert_submit_gpu.template)where user needs to put 
necessary Spark configurations. Either user can submit the `nds_transcode.py` directly to spark with
arbitary Spark parameters.

Parquet and Orc are supported for output data foramt at present.

User can also specify `--tables` to convert specific table or tables. See argument details below.

if `--floats` is specified in the command, DoubleType will be used to replace DecimalType data in Parquet files,
otherwise DecimalType will be saved.

arguments for `nds_transcode.py`:
```
python nds_transcode.py -h
usage: nds_transcode.py [-h] [--output_mode OUTPUT_MODE] [--input_suffix INPUT_SUFFIX]
                        [--log_level LOG_LEVEL] [--floats]
                        input_prefix output_prefix report_file

positional arguments:
  input_prefix          text to prepend to every input file path (e.g., "hdfs:///ds-generated-data"; the
                        default is empty)
  output_prefix         text to prepend to every output file (e.g., "hdfs:///ds-parquet"; the default is
                        empty)
  report_file           location to store a performance report(local)

optional arguments:
  -h, --help            show this help message and exit
  --output_mode {overwrite,append,ignore,error,errorifexists,default}
                        save modes as defined by https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modesdefault value is errorifexists, which is the Spark default behavior
  --output_format {parquet,orc}
                        output data format when converting CSV data sources. Now supports parquet, orc.
  --tables TABLES       specify table names by a comma seprated string. e.g. 'catalog_page,catalog_sales'.
  --input_suffix INPUT_SUFFIX
                        text to append to every input filename (e.g., ".dat"; the default is empty)
  --log_level LOG_LEVEL
                        set log level for Spark driver log. Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN(default: INFO)
  --floats              replace DecimalType with DoubleType when saving parquet files. If not specified,
                        decimal data will be saved.

```

Example command to submit via `spark-submit-template` utility:
```
./spark-submit-template convert_submit_gpu.template \
nds_transcode.py  raw_sf3k  parquet_sf3k report.txt
```

User can also use `spark-submit` to submit `nds_transcode.py` directly.

We provide two basic templates for GPU run(convert_submit_gpu.template) and CPU run(convert_submit_cpu.template).
To enable GPU run, user need to download two jars in advance to use spark-rapids plugin.

- cuDF jar: https://repo1.maven.org/maven2/ai/rapids/cudf/22.02.0/cudf-22.02.0.jar
- spark-rapids jar: https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.02.0/rapids-4-spark_2.12-22.02.0.jar

After that, please set environment variable `CUDF_JAR` and `SPARK_RAPIDS_PLUGIN_JAR` to the path where
the jars are downloaded to in spark submit templates.

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


### Generate Specific Query or Query Streams

```
usage: nds_gen_query_stream.py [-h] [--template TEMPLATE] [--streams STREAMS]
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

_After_ user generates query streams, Power Run can be executed using one of the them by submitting `nds_power.py` to Spark. 

Arguments supported for `nds_power.py`:
```
usage: nds_power.py [-h] [--output_prefix OUTPUT_PREFIX] [--output_format OUTPUT_FORMAT]
                    input_prefix query_stream_file time_log

positional arguments:
  input_prefix          text to prepend to every input file path (e.g., "hdfs:///ds-generated-data")
  query_stream_file     query stream file that contains NDS queries in specific order
  time_log              path to execution time log, only support local path.

optional arguments:
  -h, --help            show this help message and exit
  --output_prefix OUTPUT_PREFIX
                        text to prepend to every output file (e.g., "hdfs:///ds-parquet")
  --output_format OUTPUT_FORMAT
                        type of query output
  --property_file PROPERTY_FILE
                        property file for Spark configuration.

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

## Data Validation
To validate query output between Power Runs w/o GPU, we providing [nds_validate.py](nds_validate.py) to do
the job.

Arguments supported for `nds_validate.py`:
```
usage: nds_validate.py [-h] [--max_errors MAX_ERRORS] [--epsilon EPSILON] [--ignore_ordering] [--use_iterator]
                       input1 input2 input_format query_stream_file

positional arguments:
  input1                path of the first input data
  input2                path of the second input data
  input_format          data source type. e.g. parquet, orc
  query_stream_file     query stream file that contains NDS queries in specific order.

optional arguments:
  -h, --help            show this help message and exit
  --max_errors MAX_ERRORS
                        Maximum number of differences to report.
  --epsilon EPSILON     Allow for differences in precision when comparing floating point values.
  --ignore_ordering     Sort the data collected from the DataFrames before comparing them.
  --use_iterator        When set, use `toLocalIterator` to load one partition at atime into driver memory, reducing
                        memory usage at the cost of performancebecause processing will be single-threaded.
```

Example command to compare two query output data:
```
python validate.py \
query_output_cpu \
query_output_gpu \
parquet \
./nds_query_streams/query_1.sql \
--ignore_ordering
```

### NDS2.0 is using source code from TPC-DS Tool V3.2.0
