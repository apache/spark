# Spark Examples

This directory contains example programs for Apache Spark in Scala, Java, Python, and R.

## Overview

The examples demonstrate various Spark features and APIs:

- **Core Examples**: Basic RDD operations and transformations
- **SQL Examples**: DataFrame and SQL operations
- **Streaming Examples**: Stream processing with DStreams and Structured Streaming
- **MLlib Examples**: Machine learning algorithms and pipelines
- **GraphX Examples**: Graph processing algorithms

## Running Examples

### Using spark-submit

The recommended way to run examples:

```bash
# Run a Scala/Java example
./bin/run-example <class-name> [params]

# Example: Run SparkPi
./bin/run-example SparkPi 100

# Example: Run with specific master
MASTER=spark://host:7077 ./bin/run-example SparkPi 100
```

### Direct spark-submit

```bash
# Scala/Java examples
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master local[4] \
  examples/target/scala-2.13/jars/spark-examples*.jar \
  100

# Python examples
./bin/spark-submit examples/src/main/python/pi.py 100

# R examples
./bin/spark-submit examples/src/main/r/dataframe.R
```

### Interactive Shells

```bash
# Scala shell with examples on classpath
./bin/spark-shell --jars examples/target/scala-2.13/jars/spark-examples*.jar

# Python shell
./bin/pyspark
# Then run: exec(open('examples/src/main/python/pi.py').read())

# R shell
./bin/sparkR
# Then: source('examples/src/main/r/dataframe.R')
```

## Example Categories

### Core Examples

**Basic RDD Operations**

- `SparkPi`: Estimates π using Monte Carlo method
- `SparkLR`: Logistic regression using gradient descent
- `SparkKMeans`: K-means clustering
- `SparkPageRank`: PageRank algorithm implementation
- `GroupByTest`: Tests groupBy performance

**Locations:**
- Scala: `src/main/scala/org/apache/spark/examples/`
- Java: `src/main/java/org/apache/spark/examples/`
- Python: `src/main/python/`
- R: `src/main/r/`

### SQL Examples

**DataFrame and SQL Operations**

- `SparkSQLExample`: Basic DataFrame operations
- `SQLDataSourceExample`: Working with various data sources
- `RDDRelation`: Converting between RDDs and DataFrames
- `UserDefinedFunction`: Creating and using UDFs
- `CsvDataSource`: Reading and writing CSV files

**Running:**
```bash
# Scala
./bin/run-example sql.SparkSQLExample

# Python
./bin/spark-submit examples/src/main/python/sql/basic.py

# R
./bin/spark-submit examples/src/main/r/RSparkSQLExample.R
```

### Streaming Examples

**DStream Examples (Legacy)**

- `NetworkWordCount`: Count words from network stream
- `StatefulNetworkWordCount`: Stateful word count
- `RecoverableNetworkWordCount`: Checkpoint and recovery
- `KafkaWordCount`: Read from Apache Kafka
- `QueueStream`: Create DStream from queue

**Structured Streaming Examples**

- `StructuredNetworkWordCount`: Word count using Structured Streaming
- `StructuredKafkaWordCount`: Kafka integration
- `StructuredSessionization`: Session window operations

**Running:**
```bash
# DStream example
./bin/run-example streaming.NetworkWordCount localhost 9999

# Structured Streaming
./bin/run-example sql.streaming.StructuredNetworkWordCount localhost 9999

# Python Structured Streaming
./bin/spark-submit examples/src/main/python/sql/streaming/structured_network_wordcount.py localhost 9999
```

### MLlib Examples

**Classification**
- `LogisticRegressionExample`: Binary and multiclass classification
- `DecisionTreeClassificationExample`: Decision tree classifier
- `RandomForestClassificationExample`: Random forest classifier
- `GradientBoostedTreeClassifierExample`: GBT classifier
- `NaiveBayesExample`: Naive Bayes classifier

**Regression**
- `LinearRegressionExample`: Linear regression
- `DecisionTreeRegressionExample`: Decision tree regressor
- `RandomForestRegressionExample`: Random forest regressor
- `AFTSurvivalRegressionExample`: Survival regression

**Clustering**
- `KMeansExample`: K-means clustering
- `BisectingKMeansExample`: Bisecting K-means
- `GaussianMixtureExample`: Gaussian mixture model
- `LDAExample`: Latent Dirichlet Allocation

**Pipelines**
- `PipelineExample`: ML Pipeline with multiple stages
- `CrossValidatorExample`: Model selection with cross-validation
- `TrainValidationSplitExample`: Model selection with train/validation split

**Running:**
```bash
# Scala
./bin/run-example ml.LogisticRegressionExample

# Java
./bin/run-example ml.JavaLogisticRegressionExample

# Python
./bin/spark-submit examples/src/main/python/ml/logistic_regression.py
```

### GraphX Examples

**Graph Algorithms**

- `PageRankExample`: PageRank algorithm
- `ConnectedComponentsExample`: Finding connected components
- `TriangleCountExample`: Counting triangles
- `SocialNetworkExample`: Social network analysis

**Running:**
```bash
./bin/run-example graphx.PageRankExample
```

## Example Datasets

Many examples use sample data from the `data/` directory:

- `data/mllib/`: MLlib sample datasets
  - `sample_libsvm_data.txt`: LibSVM format data
  - `sample_binary_classification_data.txt`: Binary classification
  - `sample_multiclass_classification_data.txt`: Multiclass classification
  
- `data/graphx/`: GraphX sample data
  - `followers.txt`: Social network follower data
  - `users.txt`: User information

## Building Examples

### Build All Examples

```bash
# Build examples module
./build/mvn -pl examples -am package

# Skip tests
./build/mvn -pl examples -am -DskipTests package
```

### Build Specific Language Examples

The examples are compiled together, but you can run them separately by language.

## Creating Your Own Examples

### Scala Example Template

```scala
package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

object MyExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("My Example")
      .getOrCreate()
    
    try {
      // Your Spark code here
      import spark.implicits._
      val df = spark.range(100).toDF("number")
      df.show()
    } finally {
      spark.stop()
    }
  }
}
```

### Python Example Template

```python
from pyspark.sql import SparkSession

def main():
    spark = SparkSession \
        .builder \
        .appName("My Example") \
        .getOrCreate()
    
    try:
        # Your Spark code here
        df = spark.range(100)
        df.show()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

### Java Example Template

```java
package org.apache.spark.examples;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MyExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("My Example")
            .getOrCreate();
        
        try {
            // Your Spark code here
            Dataset<Row> df = spark.range(100);
            df.show();
        } finally {
            spark.stop();
        }
    }
}
```

### R Example Template

```r
library(SparkR)

sparkR.session(appName = "My Example")

# Your Spark code here
df <- createDataFrame(data.frame(number = 1:100))
head(df)

sparkR.session.stop()
```

## Example Directory Structure

```
examples/src/main/
├── java/org/apache/spark/examples/   # Java examples
│   ├── JavaSparkPi.java
│   ├── JavaWordCount.java
│   ├── ml/                           # ML examples
│   ├── sql/                          # SQL examples
│   └── streaming/                    # Streaming examples
├── python/                           # Python examples
│   ├── pi.py
│   ├── wordcount.py
│   ├── ml/                          # ML examples
│   ├── sql/                         # SQL examples
│   └── streaming/                   # Streaming examples
├── r/                               # R examples
│   ├── RSparkSQLExample.R
│   ├── ml.R
│   └── dataframe.R
└── scala/org/apache/spark/examples/ # Scala examples
    ├── SparkPi.scala
    ├── SparkLR.scala
    ├── ml/                          # ML examples
    ├── sql/                         # SQL examples
    ├── streaming/                   # Streaming examples
    └── graphx/                      # GraphX examples
```

## Common Patterns

### Reading Data

```scala
// Text file
val textData = spark.read.textFile("path/to/file.txt")

// CSV
val csvData = spark.read.option("header", "true").csv("path/to/file.csv")

// JSON
val jsonData = spark.read.json("path/to/file.json")

// Parquet
val parquetData = spark.read.parquet("path/to/file.parquet")
```

### Writing Data

```scala
// Save as text
df.write.text("output/path")

// Save as CSV
df.write.option("header", "true").csv("output/path")

// Save as Parquet
df.write.parquet("output/path")

// Save as JSON
df.write.json("output/path")
```

### Working with Partitions

```scala
// Repartition for more parallelism
val repartitioned = df.repartition(10)

// Coalesce to reduce partitions
val coalesced = df.coalesce(2)

// Partition by column when writing
df.write.partitionBy("year", "month").parquet("output/path")
```

## Performance Tips for Examples

1. **Use Local Mode for Testing**: Start with `local[*]` for development
2. **Adjust Partitions**: Use appropriate partition counts for your data size
3. **Cache When Reusing**: Cache DataFrames/RDDs that are accessed multiple times
4. **Monitor Jobs**: Use Spark UI at http://localhost:4040 to monitor execution

## Troubleshooting

### Common Issues

**OutOfMemoryError**
```bash
# Increase driver memory
./bin/spark-submit --driver-memory 4g examples/...

# Increase executor memory
./bin/spark-submit --executor-memory 4g examples/...
```

**Class Not Found**
```bash
# Make sure examples JAR is built
./build/mvn -pl examples -am package
```

**File Not Found**
```bash
# Use absolute paths or ensure working directory is spark root
./bin/run-example SparkPi  # Run from spark root directory
```

## Additional Resources

- [Quick Start Guide](../docs/quick-start.md)
- [Programming Guide](../docs/programming-guide.md)
- [SQL Programming Guide](../docs/sql-programming-guide.md)
- [MLlib Guide](../docs/ml-guide.md)
- [Structured Streaming Guide](../docs/structured-streaming-programming-guide.md)
- [GraphX Guide](../docs/graphx-programming-guide.md)

## Contributing Examples

When adding new examples:

1. Follow existing code style and structure
2. Include clear comments explaining the example
3. Add appropriate documentation
4. Test the example with various inputs
5. Add to the appropriate category
6. Update this README

For more information, see [CONTRIBUTING.md](../CONTRIBUTING.md).
