# Apache Spark Architecture

This document provides an overview of the Apache Spark architecture and its key components.

## Table of Contents

- [Overview](#overview)
- [Core Components](#core-components)
- [Execution Model](#execution-model)
- [Key Subsystems](#key-subsystems)
- [Data Flow](#data-flow)
- [Module Structure](#module-structure)

## Overview

Apache Spark is a unified analytics engine for large-scale data processing. It provides high-level APIs in Scala, Java, Python, and R, and an optimized engine that supports general computation graphs for data analysis.

### Design Principles

1. **Unified Engine**: Single system for batch processing, streaming, machine learning, and graph processing
2. **In-Memory Computing**: Leverages RAM for fast iterative algorithms and interactive queries
3. **Lazy Evaluation**: Operations are not executed until an action is called
4. **Fault Tolerance**: Resilient Distributed Datasets (RDDs) provide automatic fault recovery
5. **Scalability**: Scales from a single machine to thousands of nodes

## Core Components

### 1. Spark Core

The foundation of the Spark platform, providing:

- **Task scheduling and dispatch**
- **Memory management**
- **Fault recovery**
- **Interaction with storage systems**
- **RDD API** - The fundamental data abstraction

Location: `core/` directory

Key classes:
- `SparkContext`: Main entry point for Spark functionality
- `RDD`: Resilient Distributed Dataset, the fundamental data structure
- `DAGScheduler`: Schedules stages based on DAG of operations
- `TaskScheduler`: Launches tasks on executors

### 2. Spark SQL

Module for structured data processing with:

- **DataFrame and Dataset APIs**
- **SQL query engine**
- **Data source connectors** (Parquet, JSON, JDBC, etc.)
- **Catalyst optimizer** for query optimization

Location: `sql/` directory

Key components:
- Query parsing and analysis
- Logical and physical query planning
- Code generation for efficient execution
- Catalog management

### 3. Spark Streaming

Framework for scalable, high-throughput, fault-tolerant stream processing:

- **DStreams** (Discretized Streams) - Legacy API
- **Structured Streaming** - Modern streaming API built on Spark SQL

Location: `streaming/` directory

Key features:
- Micro-batch processing model
- Exactly-once semantics
- Integration with Kafka, Flume, Kinesis, and more

### 4. MLlib (Machine Learning Library)

Scalable machine learning library providing:

- **Classification and regression**
- **Clustering**
- **Collaborative filtering**
- **Dimensionality reduction**
- **Feature extraction and transformation**
- **ML Pipelines** for building workflows

Location: `mllib/` and `mllib-local/` directories

### 5. GraphX

Graph processing framework with:

- **Graph abstraction** built on top of RDDs
- **Graph algorithms** (PageRank, connected components, triangle counting, etc.)
- **Pregel-like API** for iterative graph computations

Location: `graphx/` directory

## Execution Model

### Spark Application Lifecycle

1. **Initialization**: User creates a `SparkContext` or `SparkSession`
2. **Job Submission**: Actions trigger job submission to the DAG scheduler
3. **Stage Creation**: DAG scheduler breaks jobs into stages based on shuffle boundaries
4. **Task Scheduling**: Task scheduler assigns tasks to executors
5. **Execution**: Executors run tasks and return results
6. **Result Collection**: Results are collected back to the driver or written to storage

### Driver and Executors

- **Driver Program**: Runs the main() function and creates SparkContext
  - Converts user program into tasks
  - Schedules tasks on executors
  - Maintains metadata about the application

- **Executors**: Processes that run on worker nodes
  - Run tasks assigned by the driver
  - Store data in memory or disk
  - Return results to the driver

### Cluster Managers

Spark supports multiple cluster managers:

- **Standalone**: Built-in cluster manager
- **Apache YARN**: Hadoop's resource manager
- **Apache Mesos**: General-purpose cluster manager
- **Kubernetes**: Container orchestration platform

Location: `resource-managers/` directory

## Key Subsystems

### Memory Management

Spark manages memory in several regions:

1. **Execution Memory**: For shuffles, joins, sorts, and aggregations
2. **Storage Memory**: For caching and broadcasting data
3. **User Memory**: For user data structures and metadata
4. **Reserved Memory**: System reserved memory

Configuration: Unified memory management allows dynamic allocation between execution and storage.

### Shuffle Subsystem

Handles data redistribution across partitions:

- **Shuffle Write**: Map tasks write data to local disk
- **Shuffle Read**: Reduce tasks fetch data from map outputs
- **Shuffle Service**: External shuffle service for improved reliability

Location: `core/src/main/scala/org/apache/spark/shuffle/`

### Storage Subsystem

Manages cached data and intermediate results:

- **Block Manager**: Manages storage of data blocks
- **Memory Store**: In-memory cache
- **Disk Store**: Disk-based storage
- **Off-Heap Storage**: Direct memory storage

Location: `core/src/main/scala/org/apache/spark/storage/`

### Serialization

Efficient serialization is critical for performance:

- **Java Serialization**: Default, but slower
- **Kryo Serialization**: Faster and more compact (recommended)
- **Custom Serializers**: For specific data types

Location: `core/src/main/scala/org/apache/spark/serializer/`

## Data Flow

### Transformation and Action Pipeline

1. **Transformations**: Lazy operations that define a new RDD/DataFrame
   - Examples: `map`, `filter`, `join`, `groupBy`
   - Build up a DAG of operations

2. **Actions**: Operations that trigger computation
   - Examples: `count`, `collect`, `save`, `reduce`
   - Cause DAG execution

3. **Stages**: Groups of tasks that can be executed together
   - Separated by shuffle operations
   - Pipeline operations within a stage

4. **Tasks**: Unit of work sent to executors
   - One task per partition
   - Execute transformations and return results

## Module Structure

### Project Organization

```
spark/
├── assembly/          # Builds the final Spark assembly JAR
├── bin/              # User-facing command-line scripts
├── build/            # Build-related scripts
├── common/           # Common utilities shared across modules
├── conf/             # Configuration file templates
├── connector/        # External data source connectors
├── core/             # Spark Core engine
├── data/             # Sample data for examples
├── dev/              # Development scripts and tools
├── docs/             # Documentation source files
├── examples/         # Example programs
├── graphx/           # Graph processing library
├── hadoop-cloud/     # Cloud storage integration
├── launcher/         # Application launcher
├── mllib/            # Machine learning library (RDD-based)
├── mllib-local/      # Local ML algorithms
├── python/           # PySpark - Python API
├── R/                # SparkR - R API
├── repl/             # Interactive Scala shell
├── resource-managers/ # Cluster manager integrations
├── sbin/             # Admin scripts for cluster management
├── sql/              # Spark SQL and DataFrames
├── streaming/        # Streaming processing
└── tools/            # Various utility tools
```

### Module Dependencies

- **Core**: Foundation for all other modules
- **SQL**: Depends on Core, used by Streaming, MLlib
- **Streaming**: Depends on Core and SQL
- **MLlib**: Depends on Core and SQL
- **GraphX**: Depends on Core
- **Python/R**: Language bindings to Core APIs

## Building and Testing

For detailed build instructions, see [building-spark.md](docs/building-spark.md).

Quick start:
```bash
# Build Spark
./build/mvn -DskipTests clean package

# Run tests
./dev/run-tests

# Run specific module tests
./build/mvn test -pl core
```

## Performance Tuning

Key areas for optimization:

1. **Memory Configuration**: Adjust executor memory and memory fractions
2. **Parallelism**: Set appropriate partition counts
3. **Serialization**: Use Kryo for better performance
4. **Caching**: Cache frequently accessed data
5. **Broadcast Variables**: Efficiently distribute large read-only data
6. **Data Locality**: Ensure tasks run close to their data

See [tuning.md](docs/tuning.md) for detailed tuning guidelines.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and the [contributing guide](https://spark.apache.org/contributing.html) for information on how to contribute to Apache Spark.

## Further Reading

- [Programming Guide](docs/programming-guide.md)
- [SQL Programming Guide](docs/sql-programming-guide.md)
- [Structured Streaming Guide](docs/structured-streaming-programming-guide.md)
- [MLlib Guide](docs/ml-guide.md)
- [GraphX Guide](docs/graphx-programming-guide.md)
- [Cluster Overview](docs/cluster-overview.md)
- [Configuration](docs/configuration.md)
