# Spark Core

Spark Core is the foundation of the Apache Spark platform. It provides the basic functionality for distributed task dispatching, scheduling, and I/O operations.

## Overview

Spark Core contains the fundamental abstractions and components that all other Spark modules build upon:

- **Resilient Distributed Datasets (RDDs)**: The fundamental data abstraction in Spark
- **SparkContext**: The main entry point for Spark functionality
- **Task Scheduling**: DAG scheduler and task scheduler for distributed execution
- **Memory Management**: Unified memory management for execution and storage
- **Shuffle System**: Data redistribution across partitions
- **Storage System**: In-memory and disk-based storage for cached data
- **Network Communication**: RPC and data transfer between driver and executors

## Key Components

### RDD (Resilient Distributed Dataset)

The core abstraction in Spark - an immutable, distributed collection of objects that can be processed in parallel.

**Key characteristics:**
- **Resilient**: Fault-tolerant through lineage information
- **Distributed**: Data is partitioned across cluster nodes
- **Immutable**: Cannot be changed once created

**Location**: `src/main/scala/org/apache/spark/rdd/`

**Main classes:**
- `RDD.scala`: Base RDD class with transformations and actions
- `HadoopRDD.scala`: RDD for reading from Hadoop
- `ParallelCollectionRDD.scala`: RDD created from a local collection
- `MapPartitionsRDD.scala`: Result of map-like transformations

### SparkContext

The main entry point for Spark functionality. Creates RDDs, accumulators, and broadcast variables.

**Location**: `src/main/scala/org/apache/spark/SparkContext.scala`

**Key responsibilities:**
- Connects to cluster manager
- Acquires executors
- Sends application code to executors
- Creates and manages RDDs
- Schedules and executes jobs

### Scheduling

#### DAGScheduler

Computes a DAG of stages for each job and submits them to the TaskScheduler.

**Location**: `src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala`

**Responsibilities:**
- Determines preferred locations for tasks based on cache status
- Handles task failures and stage retries
- Identifies shuffle boundaries to split stages
- Manages job completion and failure

#### TaskScheduler

Submits task sets to the cluster, manages task execution, and retries failed tasks.

**Location**: `src/main/scala/org/apache/spark/scheduler/TaskScheduler.scala`

**Implementations:**
- `TaskSchedulerImpl`: Default implementation
- `YarnScheduler`: YARN-specific implementation
- Cluster manager-specific schedulers

### Memory Management

Unified memory management system that dynamically allocates memory between execution and storage.

**Location**: `src/main/scala/org/apache/spark/memory/`

**Components:**
- `MemoryManager`: Base memory management interface
- `UnifiedMemoryManager`: Dynamic allocation between execution and storage
- `StorageMemoryPool`: Memory pool for caching
- `ExecutionMemoryPool`: Memory pool for shuffles and joins

**Memory regions:**
1. **Execution Memory**: Shuffles, joins, sorts, aggregations
2. **Storage Memory**: Caching and broadcasting
3. **User Memory**: User data structures
4. **Reserved Memory**: System overhead

### Shuffle System

Handles data redistribution between stages.

**Location**: `src/main/scala/org/apache/spark/shuffle/`

**Key classes:**
- `ShuffleManager`: Interface for shuffle implementations
- `SortShuffleManager`: Default shuffle implementation
- `ShuffleWriter`: Writes shuffle data
- `ShuffleReader`: Reads shuffle data

**Shuffle process:**
1. **Shuffle Write**: Map tasks write partitioned data to disk
2. **Shuffle Fetch**: Reduce tasks fetch data from map outputs
3. **Shuffle Service**: External service for serving shuffle data

### Storage System

Block-based storage abstraction for cached data and shuffle outputs.

**Location**: `src/main/scala/org/apache/spark/storage/`

**Components:**
- `BlockManager`: Manages data blocks in memory and disk
- `MemoryStore`: In-memory block storage
- `DiskStore`: Disk-based block storage
- `BlockManagerMaster`: Master for coordinating block managers

**Storage levels:**
- `MEMORY_ONLY`: Store in memory only
- `MEMORY_AND_DISK`: Spill to disk if memory is full
- `DISK_ONLY`: Store on disk only
- `OFF_HEAP`: Store in off-heap memory

### Network Layer

Communication infrastructure for driver-executor and executor-executor communication.

**Location**: `src/main/scala/org/apache/spark/network/` and `common/network-*/`

**Components:**
- `NettyRpcEnv`: Netty-based RPC implementation
- `TransportContext`: Network communication setup
- `BlockTransferService`: Block data transfer

### Serialization

Efficient serialization for data and closures.

**Location**: `src/main/scala/org/apache/spark/serializer/`

**Serializers:**
- `JavaSerializer`: Default Java serialization (slower)
- `KryoSerializer`: Faster, more compact serialization (recommended)

**Configuration:**
```scala
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

## API Overview

### Creating RDDs

```scala
// From a local collection
val data = Array(1, 2, 3, 4, 5)
val rdd = sc.parallelize(data)

// From external storage
val textFile = sc.textFile("hdfs://path/to/file")

// From another RDD
val mapped = rdd.map(_ * 2)
```

### Transformations

Lazy operations that define a new RDD:

```scala
val mapped = rdd.map(x => x * 2)
val filtered = rdd.filter(x => x > 10)
val flatMapped = rdd.flatMap(x => x.toString.split(" "))
```

### Actions

Operations that trigger computation:

```scala
val count = rdd.count()
val collected = rdd.collect()
val reduced = rdd.reduce(_ + _)
rdd.saveAsTextFile("hdfs://path/to/output")
```

### Caching

```scala
// Cache in memory
rdd.cache()

// Cache with specific storage level
rdd.persist(StorageLevel.MEMORY_AND_DISK)

// Remove from cache
rdd.unpersist()
```

## Configuration

Key configuration parameters (set via `SparkConf`):

### Memory
- `spark.executor.memory`: Executor memory (default: 1g)
- `spark.memory.fraction`: Fraction for execution and storage (default: 0.6)
- `spark.memory.storageFraction`: Fraction of spark.memory.fraction for storage (default: 0.5)

### Parallelism
- `spark.default.parallelism`: Default number of partitions (default: number of cores)
- `spark.sql.shuffle.partitions`: Partitions for shuffle operations (default: 200)

### Scheduling
- `spark.scheduler.mode`: FIFO or FAIR (default: FIFO)
- `spark.locality.wait`: Wait time for data-local tasks (default: 3s)

### Shuffle
- `spark.shuffle.compress`: Compress shuffle output (default: true)
- `spark.shuffle.spill.compress`: Compress shuffle spills (default: true)

See [configuration.md](../docs/configuration.md) for complete list.

## Architecture

### Job Execution Flow

1. **Action called** → Triggers job submission
2. **DAG construction** → DAGScheduler creates stages
3. **Task creation** → Each stage becomes a task set
4. **Task scheduling** → TaskScheduler assigns tasks to executors
5. **Task execution** → Executors run tasks
6. **Result collection** → Results returned to driver

### Fault Tolerance

Spark achieves fault tolerance through:

1. **RDD Lineage**: Each RDD knows how to recompute from its parent RDDs
2. **Task Retry**: Failed tasks are automatically retried
3. **Stage Retry**: Failed stages are re-executed
4. **Checkpoint**: Optionally save RDD to stable storage

## Building and Testing

### Build Core Module

```bash
# Build core only
./build/mvn -pl core -DskipTests package

# Build core with dependencies
./build/mvn -pl core -am -DskipTests package
```

### Run Tests

```bash
# Run all core tests
./build/mvn test -pl core

# Run specific test suite
./build/mvn test -pl core -Dtest=SparkContextSuite

# Run specific test
./build/mvn test -pl core -Dtest=SparkContextSuite#testJobCancellation
```

## Source Code Organization

```
core/src/main/
├── java/                    # Java sources
│   └── org/apache/spark/
│       ├── api/            # Java API
│       ├── shuffle/        # Shuffle implementation
│       └── unsafe/         # Unsafe operations
├── scala/                  # Scala sources
│   └── org/apache/spark/
│       ├── rdd/           # RDD implementations
│       ├── scheduler/     # Scheduling components
│       ├── storage/       # Storage system
│       ├── memory/        # Memory management
│       ├── shuffle/       # Shuffle system
│       ├── broadcast/     # Broadcast variables
│       ├── deploy/        # Deployment components
│       ├── executor/      # Executor implementation
│       ├── io/           # I/O utilities
│       ├── network/      # Network layer
│       ├── serializer/   # Serialization
│       └── util/         # Utilities
└── resources/            # Resource files
```

## Performance Tuning

### Memory Optimization

1. Adjust memory fractions based on workload
2. Use off-heap memory for large datasets
3. Choose appropriate storage levels
4. Avoid excessive caching

### Shuffle Optimization

1. Minimize shuffle operations
2. Use `reduceByKey` instead of `groupByKey`
3. Increase shuffle parallelism
4. Enable compression

### Serialization Optimization

1. Use Kryo serialization
2. Register custom classes with Kryo
3. Avoid closures with large objects

### Data Locality

1. Ensure data and compute are co-located
2. Increase `spark.locality.wait` if needed
3. Use appropriate storage levels

## Common Issues and Solutions

### OutOfMemoryError

- Increase executor memory
- Reduce parallelism
- Use disk-based storage levels
- Enable off-heap memory

### Shuffle Failures

- Increase shuffle memory
- Increase shuffle parallelism
- Enable external shuffle service

### Slow Performance

- Check data skew
- Optimize shuffle operations
- Increase parallelism
- Enable speculation

## Further Reading

- [RDD Programming Guide](../docs/rdd-programming-guide.md)
- [Cluster Mode Overview](../docs/cluster-overview.md)
- [Tuning Guide](../docs/tuning.md)
- [Job Scheduling](../docs/job-scheduling.md)
- [Hardware Provisioning](../docs/hardware-provisioning.md)

## Related Modules

- [common/](../common/) - Common utilities shared across modules
- [launcher/](../launcher/) - Application launcher
- [sql/](../sql/) - Spark SQL and DataFrames
- [streaming/](../streaming/) - Spark Streaming
