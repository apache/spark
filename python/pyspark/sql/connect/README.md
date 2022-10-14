# Spark Connect

**Spark Connect is a strictly experimental feature and under heavy development.
All APIs should be considered volatile and should not be used in production.**

This module contains the implementation of Spark Connect which is a logical plan
facade for the implementation in Spark. Spark Connect is directly integrated into the build
of Spark. To enable it, you only need to activate the driver plugin for Spark Connect.

## Build

```bash
./build/mvn -Phive clean package
```

or

```bash
./build/sbt -Phive clean package
```
   
## Run Spark Shell

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

## Run Tests

```bash
./python/run-tests --testnames 'pyspark.sql.tests.connect.test_connect_basic'
```

