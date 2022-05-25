## Disclaimer

NDS is derived from the TPC-DS benchmarks and as such any results obtained using NDS are not
comparable to published TPC-DS benchmark results, as the results obtained from using NDS do not
comply with the TPC-DS benchmark.

## License

NDS is licensed under Apache License, Version 2.0.

Additionally, certain files in NDS are licensed subject to the accompanying [TPC EULA](TPC%20EULA.txt) (also 
available at http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).  Files subject to the TPC 
EULA are identified as such within the files.

You may not use NDS except in compliance with the Apache License, Version 2.0 and the TPC EULA.


MapReduce TPC-DS Generator
==========================

This simplifies creating TPC-DS datasets at large scales on a Hadoop cluster.

To get set up, you need to run

```
$ make
```

This will download the TPC-DS dsgen program, compile it and use maven to build the MR app wrapped around it.

To generate the data use a variation of the following command to specify the target directory in HDFS (`-d`), the scale factor in GB (`-s 10000`, for 10TB), and the parallelism to use (`-p 100`).

```
$ hadoop jar target/tpcds-gen-1.0-SNAPSHOT.jar -d /tmp/tpc-ds/sf10000/ -p 100 -s 10000
```

This uses the existing parallelism in the driver.c of TPC-DS without modification and uses it to run the command on multiple machines instead of running in local fork mode.

The command generates multiple files for each map task, resulting in each table having its own subdirectory.

Assumptions made are that all machines in the cluster are OS/arch/lib identical.
