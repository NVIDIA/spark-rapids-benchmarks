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

Checkout to the parent folder of the repo(We assume the working directly is always the parent folder in the following sections).

```
python nds.py \
--generate data \
--dir /PATH_FOR_DATA \
--scale 100 \
--parallel 100
```



## Query Generation
The modified query templates for Spark SQL are in `query_templates_nds` folder. 

To make NDS queries runnable in Spark, we applied the following changes to original templates released in TPC-DS v3.2.0:

- convert `+` synctax for `date interval add` to [date_add()](https://spark.apache.org/docs/latest/api/sql/index.html#date_add) function supported in Spark SQL.

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