# Spark Connect

This module contains the implementation of Spark Connect which is a logical plan
facade for the implementation in Spark.


## Build

1. Build Spark as usual per the documentation.
2. Build and package the Spark Connect package
   ```commandline
   ./build/mvn -pl connect package
   ```
   
## Run Spark Shell

```commandline
./bin/spark-shell --jars ./connect/target/spark-connect_2.12-3.4.0-SNAPSHOT.jar --conf spark.plugins=org.apache.spark.sql.sparkconnect.service.SparkConnectPlugin
```

## Run Tests

1. Edit `run-tests.py` and fix the `FIXME` for the location of the spark connect JAR.



```commandline
./run-tests --testnames 'pyspark.sql.tests.connect.test_spark_connect'
```

