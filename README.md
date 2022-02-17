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
checkout to the parent folder of the repo

```
python nds.py --generate data --dir /PATH_OF_DATA --scale 100 --parallel 100
```



## Query Generation
(TODO)


## Benchmark Runner

### Power Run
(TODO)
### Throughput Run
(TODO)