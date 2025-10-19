# Apache Spark Documentation Index

This document provides a complete index of all documentation available in the Apache Spark repository.

## Quick Start

- **[README.md](README.md)** - Main project README with quick start guide
- **[docs/quick-start.md](docs/quick-start.md)** - Interactive tutorial for getting started
- **[CONTRIBUTING.md](CONTRIBUTING.md)** - How to contribute to the project

## Architecture and Development

### Core Documentation
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Complete Spark architecture overview
  - Core components and their responsibilities
  - Execution model and data flow
  - Module structure and dependencies
  - Key subsystems (memory, shuffle, storage, networking)

- **[DEVELOPMENT.md](DEVELOPMENT.md)** - Developer guide
  - Setting up development environment
  - Building and testing instructions
  - IDE configuration
  - Code style guidelines
  - Debugging techniques
  - Common development tasks

- **[CODE_DOCUMENTATION_GUIDE.md](CODE_DOCUMENTATION_GUIDE.md)** - Code documentation standards
  - Scaladoc guidelines
  - Javadoc guidelines
  - Python docstring conventions
  - R documentation standards
  - Best practices and examples

## Module Documentation

### Core Modules

#### Spark Core
- **[core/README.md](core/README.md)** - Spark Core documentation
  - RDD API and operations
  - SparkContext and configuration
  - Task scheduling (DAGScheduler, TaskScheduler)
  - Memory management
  - Shuffle system
  - Storage system
  - Serialization

#### Spark SQL
- **[sql/README.md](sql/README.md)** - Spark SQL documentation (if exists)
- **[docs/sql-programming-guide.md](docs/sql-programming-guide.md)** - SQL programming guide
- **[docs/sql-data-sources.md](docs/sql-data-sources.md)** - Data source integration
- **[docs/sql-performance-tuning.md](docs/sql-performance-tuning.md)** - Performance tuning

#### Streaming
- **[streaming/README.md](streaming/README.md)** - Spark Streaming documentation
  - DStreams API (legacy)
  - Structured Streaming (recommended)
  - Input sources and output sinks
  - Windowing and stateful operations
  - Performance tuning

#### MLlib
- **[mllib/README.md](mllib/README.md)** - MLlib documentation
  - ML Pipeline API (spark.ml)
  - RDD-based API (spark.mllib - maintenance mode)
  - Classification and regression algorithms
  - Clustering algorithms
  - Feature engineering
  - Model selection and tuning

#### GraphX
- **[graphx/README.md](graphx/README.md)** - GraphX documentation
  - Property graphs
  - Graph operators
  - Graph algorithms (PageRank, Connected Components, etc.)
  - Pregel API
  - Performance optimization

### Common Modules
- **[common/README.md](common/README.md)** - Common utilities documentation
  - Network communication (network-common, network-shuffle)
  - Key-value store
  - Sketching algorithms
  - Unsafe operations

### Tools and Utilities

#### User-Facing Tools
- **[bin/README.md](bin/README.md)** - User scripts documentation
  - spark-submit: Application submission
  - spark-shell: Interactive Scala shell
  - pyspark: Interactive Python shell
  - sparkR: Interactive R shell
  - spark-sql: SQL query shell
  - run-example: Example runner

#### Administration Tools
- **[sbin/README.md](sbin/README.md)** - Admin scripts documentation
  - Cluster management scripts
  - start-all.sh / stop-all.sh
  - Master and worker daemon management
  - History server setup
  - Standalone cluster configuration

#### Programmatic API
- **[launcher/README.md](launcher/README.md)** - Launcher API documentation
  - SparkLauncher for programmatic application launching
  - SparkAppHandle for monitoring
  - Integration patterns

#### Resource Managers
- **[resource-managers/README.md](resource-managers/README.md)** - Resource manager integrations
  - YARN integration
  - Kubernetes integration
  - Mesos integration
  - Comparison and configuration

### Examples
- **[examples/README.md](examples/README.md)** - Example programs
  - Core examples (RDD operations)
  - SQL examples (DataFrames)
  - Streaming examples
  - MLlib examples
  - GraphX examples
  - Running examples

## Official Documentation

### Programming Guides
- **[docs/programming-guide.md](docs/programming-guide.md)** - General programming guide
- **[docs/rdd-programming-guide.md](docs/rdd-programming-guide.md)** - RDD programming
- **[docs/sql-programming-guide.md](docs/sql-programming-guide.md)** - SQL programming
- **[docs/structured-streaming-programming-guide.md](docs/structured-streaming-programming-guide.md)** - Structured Streaming
- **[docs/streaming-programming-guide.md](docs/streaming-programming-guide.md)** - DStreams (legacy)
- **[docs/ml-guide.md](docs/ml-guide.md)** - Machine learning guide
- **[docs/graphx-programming-guide.md](docs/graphx-programming-guide.md)** - Graph processing

### Deployment
- **[docs/cluster-overview.md](docs/cluster-overview.md)** - Cluster mode overview
- **[docs/submitting-applications.md](docs/submitting-applications.md)** - Application submission
- **[docs/spark-standalone.md](docs/spark-standalone.md)** - Standalone cluster mode
- **[docs/running-on-yarn.md](docs/running-on-yarn.md)** - Running on YARN
- **[docs/running-on-kubernetes.md](docs/running-on-kubernetes.md)** - Running on Kubernetes

### Configuration and Tuning
- **[docs/configuration.md](docs/configuration.md)** - Configuration reference
- **[docs/tuning.md](docs/tuning.md)** - Performance tuning guide
- **[docs/hardware-provisioning.md](docs/hardware-provisioning.md)** - Hardware recommendations
- **[docs/job-scheduling.md](docs/job-scheduling.md)** - Job scheduling
- **[docs/monitoring.md](docs/monitoring.md)** - Monitoring and instrumentation

### Advanced Topics
- **[docs/security.md](docs/security.md)** - Security guide
- **[docs/cloud-integration.md](docs/cloud-integration.md)** - Cloud storage integration
- **[docs/building-spark.md](docs/building-spark.md)** - Building from source

### Migration Guides
- **[docs/core-migration-guide.md](docs/core-migration-guide.md)** - Core API migration
- **[docs/sql-migration-guide.md](docs/sql-migration-guide.md)** - SQL migration
- **[docs/ml-migration-guide.md](docs/ml-migration-guide.md)** - MLlib migration
- **[docs/pyspark-migration-guide.md](docs/pyspark-migration-guide.md)** - PySpark migration
- **[docs/ss-migration-guide.md](docs/ss-migration-guide.md)** - Structured Streaming migration

### API References
- **[docs/sql-ref.md](docs/sql-ref.md)** - SQL reference
- **[docs/sql-ref-functions.md](docs/sql-ref-functions.md)** - SQL functions
- **[docs/sql-ref-datatypes.md](docs/sql-ref-datatypes.md)** - SQL data types
- **[docs/sql-ref-syntax.md](docs/sql-ref-syntax.md)** - SQL syntax

## Language-Specific Documentation

### Python (PySpark)
- **[python/README.md](python/README.md)** - PySpark overview
- **[python/docs/](python/docs/)** - PySpark documentation source
- **[docs/api/python/](docs/api/python/)** - Python API docs (generated)

### R (SparkR)
- **[R/README.md](R/README.md)** - SparkR overview
- **[docs/sparkr.md](docs/sparkr.md)** - SparkR guide
- **[R/pkg/README.md](R/pkg/README.md)** - R package documentation

### Scala
- **[docs/api/scala/](docs/api/scala/)** - Scala API docs (generated)

### Java
- **[docs/api/java/](docs/api/java/)** - Java API docs (generated)

## Data Sources

### Built-in Sources
- **[docs/sql-data-sources-load-save-functions.md](docs/sql-data-sources-load-save-functions.md)**
- **[docs/sql-data-sources-parquet.md](docs/sql-data-sources-parquet.md)**
- **[docs/sql-data-sources-json.md](docs/sql-data-sources-json.md)**
- **[docs/sql-data-sources-csv.md](docs/sql-data-sources-csv.md)**
- **[docs/sql-data-sources-jdbc.md](docs/sql-data-sources-jdbc.md)**
- **[docs/sql-data-sources-avro.md](docs/sql-data-sources-avro.md)**
- **[docs/sql-data-sources-orc.md](docs/sql-data-sources-orc.md)**

### External Integrations
- **[docs/streaming-kafka-integration.md](docs/streaming-kafka-integration.md)** - Kafka integration
- **[docs/streaming-kinesis-integration.md](docs/streaming-kinesis-integration.md)** - Kinesis integration
- **[docs/structured-streaming-kafka-integration.md](docs/structured-streaming-kafka-integration.md)** - Structured Streaming with Kafka

## Special Topics

### Machine Learning
- **[docs/ml-pipeline.md](docs/ml-pipeline.md)** - ML Pipelines
- **[docs/ml-features.md](docs/ml-features.md)** - Feature transformers
- **[docs/ml-classification-regression.md](docs/ml-classification-regression.md)** - Classification/Regression
- **[docs/ml-clustering.md](docs/ml-clustering.md)** - Clustering
- **[docs/ml-collaborative-filtering.md](docs/ml-collaborative-filtering.md)** - Recommendation
- **[docs/ml-tuning.md](docs/ml-tuning.md)** - Hyperparameter tuning

### Streaming
- **[docs/structured-streaming-programming-guide.md](docs/structured-streaming-programming-guide.md)** - Structured Streaming guide

### Graph Processing
- **[docs/graphx-programming-guide.md](docs/graphx-programming-guide.md)** - GraphX guide

## Additional Resources

### Community
- **[Apache Spark Website](https://spark.apache.org/)** - Official website
- **[Spark Documentation](https://spark.apache.org/documentation.html)** - Online docs
- **[Developer Tools](https://spark.apache.org/developer-tools.html)** - Developer resources
- **[Community](https://spark.apache.org/community.html)** - Mailing lists and chat

### External Links
- **[Spark JIRA](https://issues.apache.org/jira/projects/SPARK)** - Issue tracker
- **[GitHub Repository](https://github.com/apache/spark)** - Source code
- **[Stack Overflow](https://stackoverflow.com/questions/tagged/apache-spark)** - Q&A

## Document Organization

### By Audience

**For Users:**
- Quick Start Guide
- Programming Guides (SQL, Streaming, MLlib, GraphX)
- Configuration Guide
- Deployment Guides (YARN, Kubernetes)
- Examples

**For Developers:**
- ARCHITECTURE.md
- DEVELOPMENT.md
- CODE_DOCUMENTATION_GUIDE.md
- Module READMEs
- Building Guide

**For Administrators:**
- Cluster Overview
- Standalone Mode Guide
- Monitoring Guide
- Security Guide
- Admin Scripts (sbin/)

### By Topic

**Getting Started:**
1. README.md
2. docs/quick-start.md
3. docs/programming-guide.md

**Core Concepts:**
1. ARCHITECTURE.md
2. core/README.md
3. docs/rdd-programming-guide.md

**Data Processing:**
1. docs/sql-programming-guide.md
2. docs/structured-streaming-programming-guide.md
3. docs/ml-guide.md

**Deployment:**
1. docs/cluster-overview.md
2. docs/submitting-applications.md
3. docs/running-on-yarn.md or docs/running-on-kubernetes.md

**Optimization:**
1. docs/tuning.md
2. docs/sql-performance-tuning.md
3. docs/hardware-provisioning.md

## Documentation Standards

All documentation follows these principles:

1. **Clarity**: Clear, concise explanations
2. **Completeness**: Comprehensive coverage of topics
3. **Examples**: Code examples for all concepts
4. **Structure**: Consistent formatting and organization
5. **Accuracy**: Up-to-date and technically correct
6. **Accessibility**: Easy to find and navigate

## Contributing to Documentation

To contribute to Spark documentation:

1. Follow the style guides in CODE_DOCUMENTATION_GUIDE.md
2. Update relevant documentation when changing code
3. Add examples for new features
4. Test documentation builds locally
5. Submit pull requests with documentation updates

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Building Documentation

### Building User Documentation
```bash
cd docs
bundle install
bundle exec jekyll serve
# View at http://localhost:4000
```

### Building API Documentation
```bash
# Scala API docs
./build/mvn scala:doc

# Java API docs
./build/mvn javadoc:javadoc

# Python API docs
cd python/docs
make html
```

## Getting Help

If you can't find what you're looking for:

1. Check the [Documentation Index](https://spark.apache.org/documentation.html)
2. Search [Stack Overflow](https://stackoverflow.com/questions/tagged/apache-spark)
3. Ask on the [user mailing list](mailto:user@spark.apache.org)
4. Check [Spark JIRA](https://issues.apache.org/jira/projects/SPARK) for known issues

## Last Updated

This index was last updated: 2025-10-19

For the most up-to-date documentation, visit [spark.apache.org/docs/latest](https://spark.apache.org/docs/latest/).
