# Spark Connect

This module contains the implementation of Spark Connect which is a logical plan
facade for the implementation in Spark. Spark Connect is directly integrated into the build
of Spark. To enable it, you only need to activate the driver plugin for Spark Connect.


## Build

1. Build Spark as usual per the documentation.
2. Build and package the Spark Connect package
   ```commandline
   ./build/mvn package
   ```
   
## Run Spark Shell

```commandline
./bin/spark-shell --conf spark.plugins=org.apache.spark.sql.sparkconnect.service.SparkConnectPlugin
```

## Run Tests


```commandline
./run-tests --testnames 'pyspark.sql.tests.connect.test_spark_connect'
```

