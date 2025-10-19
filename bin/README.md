# Spark Binary Scripts

This directory contains user-facing command-line scripts for running Spark applications and interactive shells.

## Overview

These scripts provide convenient entry points for:
- Running Spark applications
- Starting interactive shells (Scala, Python, R, SQL)
- Managing Spark clusters
- Utility operations

## Main Scripts

### spark-submit

Submit Spark applications to a cluster.

**Usage:**
```bash
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
```

**Examples:**
```bash
# Run on local mode with 4 cores
./bin/spark-submit --class org.example.App --master local[4] app.jar

# Run on YARN cluster
./bin/spark-submit --class org.example.App --master yarn --deploy-mode cluster app.jar

# Run Python application
./bin/spark-submit --master local[2] script.py

# Run with specific memory and executor settings
./bin/spark-submit \
  --master spark://master:7077 \
  --executor-memory 4G \
  --total-executor-cores 8 \
  --class org.example.App \
  app.jar
```

**Key Options:**
- `--master`: Master URL (local, spark://, yarn, k8s://, mesos://)
- `--deploy-mode`: client or cluster
- `--class`: Application main class (for Java/Scala)
- `--name`: Application name
- `--jars`: Additional JARs to include
- `--packages`: Maven coordinates of packages
- `--conf`: Spark configuration property
- `--driver-memory`: Driver memory (e.g., 1g, 2g)
- `--executor-memory`: Executor memory
- `--executor-cores`: Cores per executor
- `--num-executors`: Number of executors (YARN only)

See [submitting-applications.md](../docs/submitting-applications.md) for complete documentation.

### spark-shell

Interactive Scala shell with Spark support.

**Usage:**
```bash
./bin/spark-shell [options]
```

**Examples:**
```bash
# Start local shell
./bin/spark-shell

# Connect to remote cluster
./bin/spark-shell --master spark://master:7077

# With specific memory
./bin/spark-shell --driver-memory 4g

# With additional packages
./bin/spark-shell --packages org.apache.spark:spark-avro_2.13:3.5.0
```

**In the shell:**
```scala
scala> val data = spark.range(1000)
scala> data.count()
res0: Long = 1000

scala> spark.read.json("data.json").show()
```

### pyspark

Interactive Python shell with PySpark support.

**Usage:**
```bash
./bin/pyspark [options]
```

**Examples:**
```bash
# Start local shell
./bin/pyspark

# Connect to remote cluster
./bin/pyspark --master spark://master:7077

# With specific Python version
PYSPARK_PYTHON=python3.11 ./bin/pyspark
```

**In the shell:**
```python
>>> df = spark.range(1000)
>>> df.count()
1000

>>> spark.read.json("data.json").show()
```

### sparkR

Interactive R shell with SparkR support.

**Usage:**
```bash
./bin/sparkR [options]
```

**Examples:**
```bash
# Start local shell
./bin/sparkR

# Connect to remote cluster
./bin/sparkR --master spark://master:7077
```

**In the shell:**
```r
> df <- createDataFrame(iris)
> head(df)
> count(df)
```

### spark-sql

Interactive SQL shell for running SQL queries.

**Usage:**
```bash
./bin/spark-sql [options]
```

**Examples:**
```bash
# Start SQL shell
./bin/spark-sql

# Connect to Hive metastore
./bin/spark-sql --conf spark.sql.warehouse.dir=/path/to/warehouse

# Run SQL file
./bin/spark-sql -f query.sql

# Execute inline query
./bin/spark-sql -e "SELECT * FROM table"
```

**In the shell:**
```sql
spark-sql> CREATE TABLE test (id INT, name STRING);
spark-sql> INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');
spark-sql> SELECT * FROM test;
```

### run-example

Run Spark example programs.

**Usage:**
```bash
./bin/run-example <class> [params]
```

**Examples:**
```bash
# Run SparkPi example
./bin/run-example SparkPi 100

# Run with specific master
MASTER=spark://master:7077 ./bin/run-example SparkPi

# Run SQL example
./bin/run-example sql.SparkSQLExample
```

## Utility Scripts

### spark-class

Internal script to run Spark classes. Usually not called directly by users.

**Usage:**
```bash
./bin/spark-class <class> [options]
```

### load-spark-env.sh

Loads Spark environment variables from conf/spark-env.sh. Sourced by other scripts.

## Configuration

Scripts read configuration from:

1. **Environment variables**: Set in shell or `conf/spark-env.sh`
2. **Command-line options**: Passed via `--conf` or specific flags
3. **Configuration files**: `conf/spark-defaults.conf`

### Common Environment Variables

```bash
# Java
export JAVA_HOME=/path/to/java

# Spark
export SPARK_HOME=/path/to/spark
export SPARK_MASTER_HOST=master-hostname
export SPARK_MASTER_PORT=7077

# Python
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Memory
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=4g

# Logging
export SPARK_LOG_DIR=/var/log/spark
```

Set these in `conf/spark-env.sh` for persistence.

## Master URLs

Scripts accept various master URL formats:

- **local**: Run locally with one worker thread
- **local[K]**: Run locally with K worker threads
- **local[*]**: Run locally with as many worker threads as cores
- **spark://HOST:PORT**: Connect to Spark standalone cluster
- **yarn**: Connect to YARN cluster
- **k8s://HOST:PORT**: Connect to Kubernetes cluster
- **mesos://HOST:PORT**: Connect to Mesos cluster

## Advanced Usage

### Configuring Logging

Create `conf/log4j2.properties`:
```properties
rootLogger.level = info
logger.spark.name = org.apache.spark
logger.spark.level = warn
```

### Using with Jupyter Notebook

```bash
# Set environment variables
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'

# Start PySpark (opens Jupyter)
./bin/pyspark
```

### Connecting to Remote Clusters

```bash
# Standalone cluster
./bin/spark-submit --master spark://master:7077 app.jar

# YARN
./bin/spark-submit --master yarn --deploy-mode cluster app.jar

# Kubernetes
./bin/spark-submit --master k8s://https://k8s-api:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=spark:3.5.0 \
  app.jar
```

### Dynamic Resource Allocation

```bash
./bin/spark-submit \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=10 \
  app.jar
```

## Debugging

### Enable Verbose Output

```bash
./bin/spark-submit --verbose ...
```

### Check Spark Configuration

```bash
./bin/spark-submit --class org.example.App app.jar 2>&1 | grep -i "spark\."
```

### Remote Debugging

```bash
# Driver debugging
./bin/spark-submit \
  --conf spark.driver.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005" \
  app.jar

# Executor debugging
./bin/spark-submit \
  --conf spark.executor.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006" \
  app.jar
```

## Security

### Kerberos Authentication

```bash
./bin/spark-submit \
  --principal user@REALM \
  --keytab /path/to/user.keytab \
  --master yarn \
  app.jar
```

### SSL Configuration

```bash
./bin/spark-submit \
  --conf spark.ssl.enabled=true \
  --conf spark.ssl.keyStore=/path/to/keystore \
  --conf spark.ssl.keyStorePassword=password \
  app.jar
```

## Performance Tuning

### Memory Configuration

```bash
./bin/spark-submit \
  --driver-memory 4g \
  --executor-memory 8g \
  --conf spark.memory.fraction=0.8 \
  app.jar
```

### Parallelism

```bash
./bin/spark-submit \
  --conf spark.default.parallelism=100 \
  --conf spark.sql.shuffle.partitions=200 \
  app.jar
```

### Serialization

```bash
./bin/spark-submit \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  app.jar
```

## Troubleshooting

### Common Issues

**Java not found:**
```bash
export JAVA_HOME=/path/to/java
```

**Class not found:**
```bash
# Add dependencies
./bin/spark-submit --jars dependency.jar app.jar
```

**Out of memory:**
```bash
# Increase memory
./bin/spark-submit --driver-memory 8g --executor-memory 16g app.jar
```

**Connection refused:**
```bash
# Check master URL and firewall settings
# Verify master is running with: jps | grep Master
```

## Script Internals

### Script Hierarchy

```
spark-submit
├── spark-class
│   └── load-spark-env.sh
└── Actual Java/Python execution
```

### How spark-submit Works

1. Parse command-line arguments
2. Load configuration from `spark-defaults.conf`
3. Set up classpath and Java options
4. Call `spark-class` with appropriate arguments
5. Launch JVM with Spark application

## Related Scripts

For cluster management scripts, see [../sbin/README.md](../sbin/README.md).

## Further Reading

- [Submitting Applications](../docs/submitting-applications.md)
- [Spark Configuration](../docs/configuration.md)
- [Cluster Mode Overview](../docs/cluster-overview.md)
- [Running on YARN](../docs/running-on-yarn.md)
- [Running on Kubernetes](../docs/running-on-kubernetes.md)

## Examples

More examples in [../examples/](../examples/).
