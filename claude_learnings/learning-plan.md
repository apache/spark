# Apache Spark Learning Plan

## Overview

Apache Spark is a unified analytics engine for large-scale data processing. This guide focuses on the core engine, SQL/DataFrames, and streaming components.

## High-Level Architecture

Spark has three main layers relevant to this learning plan:

1. **Core Engine** - Foundation for distributed computing (RDD, SparkContext, scheduling, execution)
2. **SQL & DataFrames** - High-level structured data processing with Catalyst optimizer
3. **Streaming** - Real-time data processing (DStream and Structured Streaming)

## Key Abstractions

- **RDD (Resilient Distributed Dataset)** - Immutable, partitioned, fault-tolerant collections
- **DataFrame** - Distributed collection of rows with schema (Dataset[Row])
- **Dataset[T]** - Strongly-typed distributed collections with optimization
- **SparkContext** - Connection to cluster, manages execution
- **SparkSession** - Unified entry point for DataFrame/SQL operations

## Learning Path

### Week 1: Core Fundamentals

**Goal:** Understand the foundation - RDDs and cluster basics

1. **Documentation**
   - [docs/cluster-overview.md](docs/cluster-overview.md) - Architecture overview
   - [docs/rdd-programming-guide.md](docs/rdd-programming-guide.md) - RDD concepts

2. **Core Files**
   - [core/src/main/scala/org/apache/spark/SparkContext.scala](core/src/main/scala/org/apache/spark/SparkContext.scala) - Entry point, cluster connection
   - [core/src/main/scala/org/apache/spark/rdd/RDD.scala](core/src/main/scala/org/apache/spark/rdd/RDD.scala) - Core abstraction (2,207 lines)

3. **Key Concepts to Understand**
   - Transformations vs actions (lazy vs eager)
   - Partitions and parallelism
   - Dependencies (narrow vs wide)
   - Lineage and fault tolerance

4. **Hands-On**
   - Run `./bin/spark-shell`
   - Try basic RDD operations (map, filter, reduce)
   - Experiment with `./bin/run-example SparkPi`

### Week 2: Execution Engine

**Goal:** Learn how Spark converts user code into distributed tasks

1. **Scheduling Layer**
   - [core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala](core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala) - Converts RDD DAG into stages
   - [core/src/main/scala/org/apache/spark/scheduler/TaskScheduler.scala](core/src/main/scala/org/apache/spark/scheduler/TaskScheduler.scala) - Task distribution
   - [core/src/main/scala/org/apache/spark/scheduler/Stage.scala](core/src/main/scala/org/apache/spark/scheduler/Stage.scala) - Stage abstraction

2. **Execution Layer**
   - [core/src/main/scala/org/apache/spark/executor/Executor.scala](core/src/main/scala/org/apache/spark/executor/Executor.scala) - Task execution on workers
   - [core/src/main/scala/org/apache/spark/scheduler/Task.scala](core/src/main/scala/org/apache/spark/scheduler/Task.scala) - Task abstraction

3. **Key Concepts**
   - DAG (Directed Acyclic Graph) of RDD operations
   - Stages and shuffle boundaries
   - Task serialization and distribution
   - Memory management and caching

4. **Execution Flow**
   ```
   User Action → DAGScheduler → Stages → Tasks → TaskScheduler → Executors → Results
   ```

### Week 3: SQL & DataFrames Basics

**Goal:** Understand the high-level DataFrame API

1. **Entry Points**
   - [sql/api/src/main/scala/org/apache/spark/sql/SparkSession.scala](sql/api/src/main/scala/org/apache/spark/sql/SparkSession.scala) - Modern entry point
   - [sql/api/src/main/scala/org/apache/spark/sql/Dataset.scala](sql/api/src/main/scala/org/apache/spark/sql/Dataset.scala) - Typed collections
   - [sql/api/src/main/scala/org/apache/spark/sql/DataFrame.scala](sql/api/src/main/scala/org/apache/spark/sql/DataFrame.scala) - Type alias for Dataset[Row]

2. **Documentation**
   - [docs/sql-programming-guide.md](docs/sql-programming-guide.md) - SQL overview
   - [sql/README.md](sql/README.md) - SQL module overview

3. **Key Concepts**
   - DataFrames vs Datasets vs RDDs
   - Schema and types
   - DataFrame transformations (select, filter, join, groupBy)
   - SQL vs DataFrame API equivalence

4. **Hands-On**
   - Run `./bin/spark-shell` or `./bin/pyspark`
   - Load data and create DataFrames
   - Try SQL queries vs DataFrame operations
   - Examine query plans with `.explain()`

### Week 4: Catalyst Optimizer

**Goal:** Understand how Spark optimizes SQL queries

1. **Parser**
   - [sql/catalyst/parser/](sql/catalyst/parser/) - SQL text to AST
   - Uses ANTLR for parsing

2. **Logical Plans**
   - [sql/catalyst/plans/logical/](sql/catalyst/plans/logical/) - Query representation
   - Tree structure of relational operations

3. **Analysis**
   - [sql/catalyst/analysis/](sql/catalyst/analysis/) - Semantic validation
   - Resolves table names, column names, types

4. **Optimization**
   - [sql/catalyst/optimizer/](sql/catalyst/optimizer/) - Rule-based optimizations
   - Key rules: predicate pushdown, constant folding, join reordering

5. **Physical Planning**
   - [sql/catalyst/planning/QueryPlanner.scala](sql/catalyst/planning/QueryPlanner.scala) - Logical to physical
   - [sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanner.scala](sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanner.scala) - Spark-specific planner

6. **Pipeline Flow**
   ```
   SQL Text → Parser → Unresolved Logical Plan → Analyzer → Resolved Logical Plan
   → Optimizer → Optimized Logical Plan → Planner → Physical Plan → Execution
   ```

7. **Key Concepts**
   - Rule-based optimization
   - Cost-based optimization (CBO)
   - Expression trees
   - Plan transformations

### Week 5: Physical Execution (SQL)

**Goal:** Learn how optimized plans execute

1. **Physical Operators**
   - [sql/core/src/main/scala/org/apache/spark/sql/execution/](sql/core/src/main/scala/org/apache/spark/sql/execution/) - Physical operators
   - Key operators: ProjectExec, FilterExec, HashAggregateExec, SortMergeJoinExec

2. **Columnar Processing**
   - [sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/](sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/) - Columnar storage

3. **Adaptive Query Execution (AQE)**
   - [sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/](sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/) - Runtime optimizations
   - Dynamic partition pruning, join strategy changes

4. **Code Generation**
   - [sql/catalyst/expressions/codegen/](sql/catalyst/expressions/codegen/) - Whole-stage code generation
   - Tungsten execution engine

5. **Key Concepts**
   - Volcano iterator model
   - Whole-stage code generation
   - Memory management (on-heap vs off-heap)
   - Shuffle operations in SQL context

### Week 6: Streaming

**Goal:** Understand real-time data processing

1. **DStream (Original Streaming)**
   - [streaming/src/main/scala/org/apache/spark/streaming/StreamingContext.scala](streaming/src/main/scala/org/apache/spark/streaming/StreamingContext.scala) - Entry point
   - [streaming/src/main/scala/org/apache/spark/streaming/dstream/](streaming/src/main/scala/org/apache/spark/streaming/dstream/) - DStream implementations
   - Micro-batch processing model

2. **Structured Streaming (Newer)**
   - Built on top of SQL/DataFrame engine
   - [docs/structured-streaming-programming-guide.md](docs/structured-streaming-programming-guide.md)
   - Continuous vs micro-batch modes

3. **Documentation**
   - [docs/streaming-programming-guide.md](docs/streaming-programming-guide.md) - DStream guide
   - [docs/structured-streaming-programming-guide.md](docs/structured-streaming-programming-guide.md) - Structured streaming

4. **Key Concepts**
   - Micro-batching vs continuous processing
   - Windowing operations
   - State management
   - Exactly-once semantics
   - Watermarks and late data handling

5. **Comparison**
   - DStream: RDD-based, lower-level
   - Structured Streaming: DataFrame-based, optimized by Catalyst

## Important Directories

```
spark/
├── core/                    # Core engine, RDD, scheduling, execution
├── sql/
│   ├── api/                # Public DataFrame/Dataset API
│   ├── catalyst/           # Query optimizer
│   │   ├── parser/         # SQL parser
│   │   ├── analysis/       # Semantic analysis
│   │   ├── optimizer/      # Optimization rules
│   │   ├── plans/          # Query plan representations
│   │   └── expressions/    # Expression AST
│   ├── core/               # SQL execution engine
│   │   └── execution/      # Physical operators
│   └── hive/               # Hive integration
├── streaming/              # Streaming (DStream)
├── examples/               # Example programs
├── python/pyspark/         # Python API
└── docs/                   # Documentation
```

## Key Files Summary

| Component | File | Purpose | Lines |
|-----------|------|---------|-------|
| Core | SparkContext.scala | Cluster connection, RDD creation | 3,607 |
| Core | RDD.scala | Core distributed collection | 2,207 |
| Core | DAGScheduler.scala | Stage scheduling | Large |
| SQL | SparkSession.scala | DataFrame entry point | ~1,000 |
| SQL | Dataset.scala | Typed collections | ~1,000 |
| Catalyst | Optimizer.scala | Query optimization | Medium |
| Streaming | StreamingContext.scala | Streaming entry point | Medium |

## Execution Flow Summary

### RDD Path
```
SparkContext → RDD Operations → Action Triggered
→ DAGScheduler (create stages) → TaskScheduler (distribute tasks)
→ Executors (run tasks) → Results
```

### DataFrame/SQL Path
```
SparkSession → DataFrame Operations or SQL Query
→ Parser (SQL to AST)
→ Analyzer (resolve names, types)
→ Optimizer (logical optimizations)
→ Planner (logical to physical)
→ Physical Plan → Code Generation
→ DAGScheduler → TaskScheduler → Executors → Results
```

### Streaming Path
```
StreamingContext → DStream Operations → Start
→ Receiver (ingest data) → Micro-batches (RDDs)
→ RDD Processing → Output
```

## Learning Tips

1. **Top-down approach** - Start with user-facing APIs, drill down to internals
2. **Use the REPL** - Experiment with code interactively
3. **Read `.explain()`** - Examine query plans for DataFrames
4. **Trace execution** - Follow a simple operation end-to-end
5. **Focus on interfaces** - Understand contracts before implementations
6. **One path at a time** - Master either RDD or DataFrame first, not both simultaneously

## Hands-On Exercises

1. **Week 1**: Create RDDs, perform transformations, understand lazy evaluation
2. **Week 2**: Use Spark UI to examine stages and tasks
3. **Week 3**: Convert RDD operations to DataFrame equivalents
4. **Week 4**: Compare query plans for equivalent SQL queries
5. **Week 5**: Examine generated code with `.debug.codegen()`
6. **Week 6**: Build a simple streaming application

## Next Steps

After completing this learning plan:
- Dive deeper into specific areas of interest
- Contribute to Spark or build applications
- Explore advanced topics: performance tuning, deployment, monitoring
- Read Spark papers and design docs for deeper understanding
