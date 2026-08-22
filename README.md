# NVIDIA cuDF plugin for Apache Spark Benchmarks

A repo for Spark related benchmark sets and utilities using the 
[NVIDIA cuDF plugin for Apache Spark](https://github.com/NVIDIA/cudf-spark). 

## Benchmark sets:
- [NVIDIA Decision Support ( NDS )](./nds/)
- [NVIDIA Decision Support-H ( NDS-H )](./nds-h/)

Please see README in each benchmark set for more details including building instructions and usage
descriptions.

## Utilities

- [Portable YARN resource cost](./yarn-resource-cost/) attributes Spark
  application worker consumption from Spark and YARN logs across EMR,
  Dataproc, and on-premises deployments.
