# Spark Connect - Developer Documentation

**Spark Connect is a strictly experimental feature and under heavy development.
All APIs should be considered volatile and should not be used in production.**

This module contains the implementation of Spark Connect which is a logical plan
facade for the implementation in Spark. Spark Connect is directly integrated into the build
of Spark. To enable it, you only need to activate the driver plugin for Spark Connect.

The documentation linked here is specifically for developers of Spark Connect and not
directly intended to be end-user documentation.


## Getting Started 

### Build

```bash
./build/mvn -Phive clean package
```

or

```bash
./build/sbt -Phive clean package
```
   
### Run Spark Shell

To run Spark Connect you locally built:

```bash
# Scala shell
./bin/spark-shell \
  --jars `ls connector/connect/target/**/spark-connect*SNAPSHOT.jar | paste -sd ',' -` \
  --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin

# PySpark shell
./bin/pyspark \
  --jars `ls connector/connect/target/**/spark-connect*SNAPSHOT.jar | paste -sd ',' -` \
  --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin
```

To use the release version of Spark Connect:

```bash
./bin/spark-shell \
  --packages org.apache.spark:spark-connect_2.12:3.4.0 \
  --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin
```

### Run Tests

```bash
./python/run-tests --testnames 'pyspark.sql.tests.connect.test_connect_basic'
```


## Development Topics

### Generate proto generated files for the Python client
1. Install `buf version 1.8.0`: https://docs.buf.build/installation
2. Run `pip install grpcio==1.48.1 protobuf==4.21.6 mypy-protobuf==3.3.0`
3. Run `./connector/connect/dev/generate_protos.sh`
4. Optional Check `./dev/check-codegen-python.py`

### Guidelines for new clients

When contributing a new client please be aware that we strive to have a common
user experience across all languages. Please follow the below guidelines:

* [Connection string configuration](docs/client-connection-string.md)
