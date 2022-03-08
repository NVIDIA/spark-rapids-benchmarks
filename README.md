# NDS v2.0 Automation


## Data Generation

### prerequisites:

```
sudo apt install openjdk-8-jdk-headless gcc make flex bison byacc
```

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

### Generate data in Hadoop cluster

Note: please make sure you have `Hadoop binary` locally.

Checkout to the parent folder of the repo.
(We assume the working directory is always the parent folder in the following sections)

```
python nds.py \
--generate data \
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
--input-prefix hdfs://data/nds_raw \
--output-prefix hdfs://data/nds_parquet
```

We provide two basic templates for GPU run(convert_submit_gpu.template) and CPU run(convert_submit_cpu.template).
To enable GPU run, user need to download two jars in advance to use spark-rapids plugin.

- cuDF jar: https://repo1.maven.org/maven2/ai/rapids/cudf/22.02.0/cudf-22.02.0.jar
- spark-rapids jar: https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.02.0/rapids-4-spark_2.12-22.02.0.jar

The jar path will be used as a environment variable in the templates.

## Query Generation
The modified query templates for Spark SQL are in `query_templates_nds` folder. 

To make NDS queries runnable in Spark, we applied the following changes to original templates released in TPC-DS v3.2.0:

- convert `+` syntax for `date interval add` to [date_add()](https://spark.apache.org/docs/latest/api/sql/index.html#date_add) function supported in Spark SQL.

- convert `"` mark to `` ` `` mark for syntax compatibility in Spark SQL.


### Generate Specific Query

Sample command to generate query1 from template for NDS:
```
python nds.py \
--generate query \
--template query1.tpl \
--template-dir ./query_templates_nds \
--scale 100 \
--query-output-dir ./nds_queries

```

### Generate Query Streams

Sample command to generate query streams used for Power Run and Throughput Run.
```
python nds.py \
--generate streams \
--template-dir ./query_templates_nds \
--scale 100 \
--query-output-dir ./nds_query_streams \
--streams 10
```

## Benchmark Runner

### Power Run
(TODO)
### Throughput Run
(TODO)