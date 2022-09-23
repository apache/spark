
# [EXPERIMENTAL] Spark Connect

**Spark Connect is a strictly experimental feature and under heavy development.
All APIs should be considered volatile and should not be used in production.**

This module contains the implementation of Spark Connect which is a logical plan
facade for the implementation in Spark. Spark Connect is directly integrated into the build
of Spark. To enable it, you only need to activate the driver plugin for Spark Connect.




## Build

1. Build Spark as usual per the documentation.
2. Build and package the Spark Connect package
   ```bash
   ./build/mvn -Phive package
   ```
   or
   ```shell
   ./build/sbt -Phive package
   ```
   
## Run Spark Shell

```bash
./bin/spark-shell --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin
```

## Run Tests


```bash
./run-tests --testnames 'pyspark.sql.tests.connect.test_spark_connect'
```

