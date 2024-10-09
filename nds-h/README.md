# NDS-H v2.0 Automation

## Disclaimer

NDS-H is derived from the TPC-H Benchmarks and as such any results obtained using NDS-H are not
comparable to published TPC-H Benchmark results, as the results obtained from using NDS-H do not
comply with the TPC-H Benchmarks.

## License

NDS-H is licensed under Apache License, Version 2.0.

Additionally, certain files in NDS-H are licensed subject to the accompanying [TPC EULA](../TPC%20EULA.txt) (also
available at [tpc.org](http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp). Files subject to the TPC
EULA are identified as such within the files.

You may not use NDS-H except in compliance with the Apache License, Version 2.0 and the TPC EULA.

## Prerequisites

1. Python >= 3.6
2. Necessary libraries

    ```bash
    sudo locale-gen en_US.UTF-8
    sudo apt install openjdk-8-jdk-headless gcc make flex bison byacc maven
    sudo apt install dos2unix
    ```
3. Install and set up SPARK. 
    - Download latest distro from [here](https://spark.apache.org/downloads.html)
    - Preferably >= 3.4
    - Find and note SPARK_HOME ( /DOWNLOAD/LOCATION/spark-<3.4.1>-bin-hadoop3 )
    - (For local) Follow the steps mentioned [here](https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/on-premise.html#local-mode)
      for local setup
    - (For local) Update *_gpu_* --files with the getGpuResources.sh location as mentioned in the link above
    - (For local) Update spark master in shared/base.template with local[*]
    - (For local) Remove the conf - "spark.task.resource.gpu.amount=0.05" from all template files

4. Update Configuration
    - Update [shared/base.template](../shared/base.template) line 26 with the Spark home location.

5. For GPU run
    - Download the latest RAPIDS jar from [here](https://oss.sonatype.org/content/repositories/staging/com/nvidia/rapids-4-spark_2.12/)
  
    - Update [shared/base.template](../shared/base.template) line 36 with rapids plugin jar location

6. TPC-H Tools

    - Download TPC-H Tools from the [official TPC website] (https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp). The tool will be downloaded as a zip package with a random guid string prefix.

    - After unzipping it, a folder called `TPC-H V3.0.1` will be seen.

    - Set environment variable `TPCH_HOME` pointing to this directory. e.g.

      ```bash
      export TPCH_HOME='/PATH/TO/YOUR/TPC-H V3.0.1'
      ```

## Use spark-submit-template with template

To help user run NDS-H, we provide a template to define the main Spark configs for spark-submit command.
User can use different templates to run NDS with different configurations for different environment.

We create [spark-submit-template](../shared/spark-submit-template), which accepts a template file and
submit the Spark job with the configs defined in the template.

Example command to submit via `spark-submit-template` utility:

```bash
cd shared
spark-submit-template convert_submit_cpu.template \
../nds-h/nds_h_transcode.py  <raw_data_file_location>  <parquet_location> report.txt
```

We provide 2 types of template files used in different steps of NDS-H:

1. *convert_submit_*.template for converting the data by using nds_h_transcode.py
2. *power_run_*.template for power run by using nds_h_power.py

We predefine different template files for different environment.
For example, we provide below template files to run nds_h_transcode.py for different environment:

* `convert_submit_cpu.template` is for Spark CPU cluster
* `convert_submit_gpu.template` is for Spark GPU cluster

You need to choose one as your template file and modify it to fit your environment.
We define a [base.template](../shared/base.template) to help you define some basic variables for your envionment.
And all the other templates will source `base.template` to get the basic variables.
When running multiple steps of NDS-H, you only need to modify `base.template` to fit for your cluster.

## Data Generation

### Build the jar for data generation

  ```bash
  cd tpch-gen
  make
  ```

### Generate data

To generate data for local -

```bash
$ python nds_h_gen_data.py -h
usage: nds_h_gen_data.py [-h] {local,hdfs} <scale> <parallel> <data_dir> [--overwrite_output]
positional arguments:
  {local,hdfs}        file system to save the generated data.
  scale               volume of data to generate in GB.
  parallel            build data in <parallel_value> separate chunks
  data_dir            generate data in directory.

optional arguments:
  -h, --help          show this help message and exit
  --overwrite_output  overwrite if there has already existing data in the path provided
                      
```

Example command:

```bash
python nds_h_gen_data.py hdfs 100 100 /data/raw_sf100 --overwrite_output
```

### Convert DSV to Parquet or Other data sources

To do the data conversion, the `nds_h_transcode.py` need to be submitted as a Spark job. User can leverage
the [spark-submit-template](../shared/spark-submit-template) utility to simplify the submission.
The utility requires a pre-defined [template file](../shared/convert_submit_gpu.template) where user needs to put necessary Spark configurations. Alternatively user can submit the `nds_h_transcode.py` directly to spark with arbitrary Spark parameters.

DSV ( pipe ) is the default input format for data conversion, it can be overridden by `--input_format`.

```bash
cd shared
./spark-submit-template convert_submit_cpu.template ../nds-h/nds_h_transcode.py <input_data_location>
<output_data_location> <report_file_location>
```

## Query Generation

The [templates.patch](./tpch-gen/patches/template_new.patch) that contains necessary modifications to make NDS-H queries runnable in Spark will be applied automatically in the build step. The final query templates will be in folder `$TPCH_HOME/dbgen/queries` after the build process.

### Generate Specific Query or Query Streams

```text
usage: nds_h_gen_query_stream.py [-h] (--template TEMPLATE | --streams STREAMS)
                              scale output_dir

positional arguments:
  scale                assume a database of this scale factor.
  output_dir           generate query in directory.
  template | stream    generate query stream or from a template arugment

optional arguments:
  -h, --help           show this help message and exit
  --template TEMPLATE  build queries from this template. Only used to generate one query from one tempalte. 
                       This argument is mutually exclusive with --streams. It
                       is often used for test purpose.
  --streams STREAMS    generate how many query streams. This argument is mutually exclusive with --template.
```

Example command to generate one query using template 1.sql ( There are 22 default queries and templates):

```bash
cd nds-h
python nds_h_gen_query_stream.py 3000 ./query_1 --template <query_number>
```

Example command to generate 10 query streams each one of which contains all NDS-H queries but in
different order:

```bash
cd nds-h
python nds_h_gen_query_stream.py 3000 ./query_streams --streams 10
```

## Benchmark Runner

### Build Dependencies

There's a customized Spark listener used to track the Spark task status e.g. success or failed
or success with retry. The results will be recorded at the json summary files when all jobs are
finished. This is often used for test or query monitoring purpose.

To build:

```bash
cd utils/jvm_listener
mvn package
```

`benchmark-listener-1.0-SNAPSHOT.jar` will be generated in `jvm_listener/target` folder.

### Power Run

_After_ user generates query streams, Power Run can be executed using one of them by submitting `nds_h_power.py` to Spark.

Arguments supported by `nds_h_power.py`:

```text
usage: nds_h_power.py [-h] [--input_format {parquet,}] 
                           [--output_format OUTPUT_FORMAT] 
                           [--property_file PROPERTY_FILE]
                           <input_data_location> 
                           <query_stream_file>
                           <time_log_file>

positional arguments:
  input_data_location     input data location (e.g., "hdfs:///ds-generated-data").
  query_stream_file       query stream file that contains NDS-H queries in specific order
  time_log_file           path to execution time log, only support local path.

optional arguments:
  -h, --help            show this help message and exit
  --input_format {parquet,orc,avro,csv,json,iceberg,delta}
                        type for input data source, e.g. parquet, orc, json, csv or iceberg, delta. Certain types are not fully supported by GPU reading, please refer to https://github.com/NVIDIA/spark-rapids/blob/branch-24.08/docs/compatibility.md for more details.
  --output_prefix OUTPUT_PREFIX
                        text to prepend to every output file (e.g., "hdfs:///ds-parquet")
  --output_format OUTPUT_FORMAT
                        type of query output
  --property_file PROPERTY_FILE
                        property file for Spark configuration.
  --json_summary_folder JSON_SUMMARY_FOLDER
                        Empty folder/path (will create if not exist) to save JSON summary file for each query.
  --sub_queries SUB_QUERIES
                        comma separated list of queries to run. If not specified, all queries in the stream file will be run. 
                        e.g. "query1,query2,query3". Note, use "_part1","_part2" and "part_3" suffix for the following query names: query15
```

Example command to submit nds_h_power.py by spark-submit-template utility:

```bash
cd shared
./spark-submit-template power_run_gpu.template \
../nds-h/nds_h_power.py \
<parquet_folder_location> \
<query_stream_folder>/query_0.sql \
time.csv \
--property_file ../utils/properties/aqe-on.properties
```

User can also use `spark-submit` to submit `nds_h_power.py` directly.

To simplify the performance analysis process, the script will create a local CSV file to save query(including TempView creation) and corresponding execution time. Note: please use `client` mode(set in your `power_run_gpu.template` file) when running in Yarn distributed environment to make sure the time log is saved correctly in your local path.

Note the template file must follow the `spark-submit-template` utility as the _first_ argument.
All Spark configuration words (such as `--conf` and corresponding `k=v` values)  are quoted by
double quotes in the template file. Please follow the format in [power_run_gpu.template](../shared/power_run_gpu.template).

User can define the `properties` file like [aqe-on.properties](../utils/properties/aqe-on.properties). The properties will be passed to the submitted Spark job along with the configurations defined in the template file. User can define some common properties in the template file and put some other properties that usually varies in the property file.

The command above will use `collect()` action to trigger Spark job for each query. It is also supported to save query output to some place for further verification. User can also specify output format e.g. csv, parquet or orc:

```bash
./spark-submit-template power_run_gpu.template \
nds_h_power.py \
parquet_sf3k \
./nds_query_streams/query_0.sql \
time.csv
```

## Data Validation
To validate query output between Power Runs with and without GPU, we provide [nds_h_validate.py](nds_h_validate.py) to do the job.

Arguments supported by `nds_h_validate.py`

```text
usage: nds_h_validate.py [-h] [--input1_format INPUT1_FORMAT]
                         [--input2_format INPUT2_FORMAT]
                         [--max_errors MAX_ERRORS] [--epsilon EPSILON]
                         [--ignore_ordering] [--use_iterator]
                         [--json_summary_folder JSON_SUMMARY_FOLDER]
                         [--sub_queries SUB_QUERIES]
                         input1 input2 query_stream_file

positional arguments:
  input1                path of the first input data.
  input2                path of the second input data.
  query_stream_file     query stream file that contains NDS queries in
                        specific order.

options:
  -h, --help            show this help message and exit
  --input1_format INPUT1_FORMAT
                        data source type for the first input data. e.g.
                        parquet, orc. Default is: parquet.
  --input2_format INPUT2_FORMAT
                        data source type for the second input data. e.g.
                        parquet, orc. Default is: parquet.
  --max_errors MAX_ERRORS
                        Maximum number of differences to report.
  --epsilon EPSILON     Allow for differences in precision when comparing
                        floating point values. Given 2 float numbers: 0.000001
                        and 0.000000, the diff of them is 0.000001 which is
                        less than 0.00001, so we regard this as acceptable and
                        will not report a mismatch.
  --ignore_ordering     Sort the data collected from the DataFrames before
                        comparing them.
  --use_iterator        When set, use `toLocalIterator` to load one partition
                        at a time into driver memory, reducing memory usage at
                        the cost of performance because processing will be
                        single-threaded.
  --json_summary_folder JSON_SUMMARY_FOLDER
                        path of a folder that contains json summary file for
                        each query.
  --sub_queries SUB_QUERIES
                        comma separated list of queries to compare. If not
                        specified, all queries in the stream file will be
                        compared. e.g. "query1,query2,query3". Note, use
                        "_part1" and "_part2" suffix e.g. query15_part1,
                        query15_part2
```

Example command to compare output data of two queries:

```bash
cd shared
./spark-submit-template power_run_cpu.template \
../nds-h/nds_h_validate.py \
<query_output_cpu> \
<query_output_gpu> \
<query_stream>.sql \ 
--ignore_ordering 
```