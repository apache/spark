# Arrow Cache Performance Tuning Guide

## Overview

This guide provides detailed recommendations for optimizing Apache Arrow cache performance in Apache Spark. Use these techniques to maximize throughput, minimize memory usage, and achieve the best performance for your specific workload.

## Quick Start: Recommended Configurations

### Configuration 1: Balanced (Default)
Best for: Most workloads, good starting point

```scala
spark.conf.set("spark.sql.cache.serializer",
  "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
spark.conf.set("spark.sql.arrow.compression.codec", "zstd")
spark.conf.set("spark.sql.arrow.compression.level", "3")
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "10000")
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
```

### Configuration 2: Maximum Performance
Best for: Performance-critical applications, ample memory

```scala
spark.conf.set("spark.sql.cache.serializer",
  "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
spark.conf.set("spark.sql.arrow.compression.codec", "lz4")
spark.conf.set("spark.sql.arrow.compression.level", "1")
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "20000")
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
```

### Configuration 3: Memory Optimized
Best for: Memory-constrained environments, large datasets

```scala
spark.conf.set("spark.sql.cache.serializer",
  "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
spark.conf.set("spark.sql.arrow.compression.codec", "zstd")
spark.conf.set("spark.sql.arrow.compression.level", "9")
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "5000")
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
```

## Tuning Parameters

### 1. Compression Codec

**Parameter**: `spark.sql.arrow.compression.codec`
**Default**: `zstd`
**Options**: `none`, `lz4`, `zstd`

#### Performance Characteristics

| Codec | Compression Speed | Decompression Speed | Compression Ratio | Best For |
|-------|------------------|---------------------|-------------------|----------|
| none | Fastest | Fastest | 1.0x (no compression) | Memory-rich, CPU-constrained |
| lz4 | Very Fast | Very Fast | 2-3x | Balanced performance |
| zstd | Fast | Fast | 3-5x | Memory-constrained |

#### When to Use Each

**Use `none`**:
- Abundant memory available
- CPU is the bottleneck
- Data doesn't compress well (e.g., encrypted data)
- Network/disk I/O is not a concern

```scala
spark.conf.set("spark.sql.arrow.compression.codec", "none")
```

**Use `lz4`** (Recommended for most workloads):
- Balanced performance/compression trade-off
- Real-time or latency-sensitive applications
- Data will be read multiple times

```scala
spark.conf.set("spark.sql.arrow.compression.codec", "lz4")
```

**Use `zstd`** (Default):
- Memory is limited
- High compression ratio needed
- Data will be cached for long periods
- Network/disk I/O is a bottleneck

```scala
spark.conf.set("spark.sql.arrow.compression.codec", "zstd")
```

### 2. Compression Level (zstd only)

**Parameter**: `spark.sql.arrow.compression.level`
**Default**: `3`
**Range**: `1` (fastest) to `22` (best compression)

#### Impact of Compression Level

| Level | Speed | Compression | Use Case |
|-------|-------|-------------|----------|
| 1-3 | Fast | Good | Most workloads (recommended) |
| 4-6 | Medium | Better | Memory-constrained |
| 7-9 | Slower | Best | Extreme memory pressure |
| 10+ | Very Slow | Diminishing returns | Rarely needed |

#### Tuning Strategy

```scala
// Start with default
spark.conf.set("spark.sql.arrow.compression.level", "3")

// If memory is tight, increase gradually
spark.conf.set("spark.sql.arrow.compression.level", "5")
spark.conf.set("spark.sql.arrow.compression.level", "7")

// If CPU is bottleneck, decrease
spark.conf.set("spark.sql.arrow.compression.level", "1")
```

### 3. Batch Size

**Parameter**: `spark.sql.arrow.maxRecordsPerBatch`
**Default**: `10000`
**Range**: `1000` to `100000` (practical limits)

#### Impact on Performance

**Larger batches** (15000-20000):
- ✅ Better vectorization
- ✅ Less overhead per row
- ✅ Better CPU cache utilization
- ❌ Higher memory usage
- ❌ Less parallelism

**Smaller batches** (5000-8000):
- ✅ Lower memory pressure
- ✅ Better parallelism
- ✅ Smaller GC pauses
- ❌ More overhead
- ❌ Less vectorization benefit

#### Tuning Strategy

```scala
// For memory-constrained environments
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "5000")

// For performance-critical applications
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "20000")

// For wide schemas (many columns)
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "5000")

// For narrow schemas (few columns)
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "20000")
```

### 4. Vectorized Reader

**Parameter**: `spark.sql.inMemoryColumnarStorage.enableVectorizedReader`
**Default**: `true`
**Recommended**: `true` (for most workloads)

#### When to Enable

✅ **Enable** when:
- Working with primitive types (Int, Long, Double, etc.)
- Performing columnar operations (aggregations, filters)
- Using modern CPUs with SIMD support
- Reading cached data frequently

❌ **Disable** when:
- Working primarily with complex types (nested structures)
- Row-by-row processing is required
- Compatibility with older systems needed

```scala
// Enable for best performance (recommended)
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
```

## Workload-Specific Tuning

### Workload 1: Filter-Heavy Queries

**Characteristics**: Many selective filters (WHERE clauses)

**Optimal Configuration**:
```scala
spark.conf.set("spark.sql.arrow.compression.codec", "lz4")  // Fast decompression
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "10000")  // Good balance
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")  // Vectorized filters
```

**Why**: Filter pushdown with statistics benefits most from fast decompression and vectorized execution.

### Workload 2: Large Aggregations

**Characteristics**: GROUP BY, SUM, AVG operations

**Optimal Configuration**:
```scala
spark.conf.set("spark.sql.arrow.compression.codec", "lz4")
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "20000")  // Larger batches
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")  // Critical!
```

**Why**: Aggregations benefit from larger batches and vectorized execution.

### Workload 3: Wide Tables (100+ columns)

**Characteristics**: Many columns per row

**Optimal Configuration**:
```scala
spark.conf.set("spark.sql.arrow.compression.codec", "zstd")  // Better compression
spark.conf.set("spark.sql.arrow.compression.level", "5")
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "5000")  // Smaller batches
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
```

**Why**: Wide tables consume more memory; smaller batches and better compression help.

### Workload 4: String-Heavy Data

**Characteristics**: Mostly string columns

**Optimal Configuration**:
```scala
spark.conf.set("spark.sql.arrow.compression.codec", "zstd")  // Strings compress well
spark.conf.set("spark.sql.arrow.compression.level", "5")
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "10000")
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
```

**Why**: Strings compress very well with zstd, saving significant memory.

### Workload 5: Columnar Input (Parquet/ORC)

**Characteristics**: Reading from columnar sources

**Optimal Configuration**:
```scala
spark.conf.set("spark.sql.arrow.compression.codec", "lz4")  // Fast compression/decompression
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "10000")  // Standard batch size
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
```

**Why**: Parquet/ORC use internal column vectors (not Arrow), so no zero-copy benefit. Fast codec and vectorized reads provide best performance.

## Advanced Tuning Techniques

### Technique 1: Adaptive Batch Sizing

Adjust batch size based on data characteristics:

```scala
val rowCount = df.count()
val columnCount = df.schema.length

val batchSize = (rowCount, columnCount) match {
  case (r, c) if c > 100 => 5000   // Wide tables
  case (r, c) if c > 50 => 10000   // Medium tables
  case (r, c) if r > 1000000 => 20000  // Large datasets
  case _ => 10000  // Default
}

spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", batchSize.toString)
```

### Technique 2: Schema-Aware Compression

Choose compression based on data types:

```scala
val hasStrings = df.schema.exists(_.dataType == StringType)
val hasPrimitives = df.schema.exists(f =>
  f.dataType == IntegerType || f.dataType == LongType || f.dataType == DoubleType)

val codec = (hasStrings, hasPrimitives) match {
  case (true, _) => "zstd"  // Strings compress well
  case (false, true) => "lz4"  // Primitives need speed
  case _ => "lz4"  // Default to fast
}

spark.conf.set("spark.sql.arrow.compression.codec", codec)
```

### Technique 3: Memory Budget-Based Tuning

Calculate batch size based on available memory:

```scala
val executorMemory = spark.conf.get("spark.executor.memory")  // e.g., "4g"
val memoryBytes = parseMemory(executorMemory)  // Convert to bytes
val cacheMemoryFraction = 0.6  // Spark default
val availableForCache = memoryBytes * cacheMemoryFraction

// Estimate bytes per row
val estimatedBytesPerRow = df.schema.map {
  case StructField(_, IntegerType, _, _) => 4
  case StructField(_, LongType, _, _) => 8
  case StructField(_, DoubleType, _, _) => 8
  case StructField(_, StringType, _, _) => 50  // Estimate
  case _ => 20  // Default estimate
}.sum

// Calculate batch size
val batchSize = Math.min(
  (availableForCache / (estimatedBytesPerRow * 100)).toInt,  // Conservative
  20000  // Max batch size
)

spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", batchSize.toString)
```

### Technique 4: Benchmark-Driven Tuning

Automate configuration selection:

```scala
def benchmarkConfig(df: DataFrame, config: Map[String, String]): Long = {
  config.foreach { case (k, v) => spark.conf.set(k, v) }

  val start = System.currentTimeMillis()
  df.cache()
  df.count()
  val cacheTime = System.currentTimeMillis() - start

  val queryStart = System.currentTimeMillis()
  df.filter("condition").count()
  val queryTime = System.currentTimeMillis() - queryStart

  df.unpersist()

  cacheTime + queryTime  // Total time
}

val configs = Seq(
  Map("spark.sql.arrow.compression.codec" -> "lz4"),
  Map("spark.sql.arrow.compression.codec" -> "zstd"),
  Map("spark.sql.arrow.compression.codec" -> "none")
)

val bestConfig = configs.minBy(config => benchmarkConfig(df, config))
println(s"Best config: $bestConfig")
```

## Monitoring and Observability

### Key Metrics to Monitor

1. **Cache Size**: `InMemoryRelation` size in bytes
2. **Cache Hit Rate**: Queries using cached data
3. **Compression Ratio**: Compressed size / uncompressed size
4. **Query Latency**: Time to execute cached queries
5. **Memory Pressure**: Off-heap memory usage

### Monitoring Code

```scala
def monitorArrowCache(df: DataFrame): Map[String, Any] = {
  val plan = df.queryExecution.optimizedPlan
  val cached = spark.sharedState.cacheManager.lookupCachedData(plan)

  cached.headOption.map { c =>
    val sizeInBytes = c.cachedRepresentation.sizeInBytesStats.value
    val numPartitions = c.cachedRepresentation.cacheBuilder.cachedColumnBuffers.getNumPartitions

    Map(
      "cacheSize" -> s"${sizeInBytes / (1024 * 1024)}MB",
      "numPartitions" -> numPartitions,
      "serializer" -> "Arrow"
    )
  }.getOrElse(Map("error" -> "Not cached"))
}
```

## Performance Troubleshooting

### Problem 1: High Memory Usage

**Symptoms**:
- Frequent GC pauses
- Out of memory errors
- Executors killed

**Solutions**:
```scala
// Reduce batch size
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "5000")

// Increase compression
spark.conf.set("spark.sql.arrow.compression.codec", "zstd")
spark.conf.set("spark.sql.arrow.compression.level", "7")
```

### Problem 2: Slow Cache Writes

**Symptoms**:
- cache() + count() takes long time
- High CPU during caching

**Solutions**:
```scala
// Use faster compression
spark.conf.set("spark.sql.arrow.compression.codec", "lz4")

// Increase batch size (if memory allows)
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "15000")
```

### Problem 3: Slow Cache Reads

**Symptoms**:
- Queries on cached data are slow
- CPU not fully utilized

**Solutions**:
```scala
// Enable vectorization
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")

// Use faster decompression
spark.conf.set("spark.sql.arrow.compression.codec", "lz4")
```

### Problem 4: Poor Compression Ratio

**Symptoms**:
- Cache size larger than expected
- Running out of memory

**Solutions**:
```scala
// Use better compression
spark.conf.set("spark.sql.arrow.compression.codec", "zstd")
spark.conf.set("spark.sql.arrow.compression.level", "9")
```

## Best Practices Summary

1. **Start with defaults**, then tune based on metrics
2. **Enable vectorized reader** for most workloads
3. **Use lz4** for performance, **zstd** for memory efficiency
4. **Monitor memory usage** and adjust batch size accordingly
5. **Test configuration changes** with representative workloads
6. **Document your tuning decisions** for future reference
7. **Re-tune periodically** as data characteristics change

## Configuration Checklist

- [ ] Compression codec selected based on workload
- [ ] Compression level tuned (if using zstd)
- [ ] Batch size appropriate for memory budget
- [ ] Vectorized reader enabled
- [ ] Configuration tested with real workload
- [ ] Metrics collection in place
- [ ] Performance baselines established
- [ ] Tuning decisions documented

## Conclusion

Arrow cache performance tuning is an iterative process. Start with recommended configurations, monitor metrics, and adjust based on your specific workload characteristics. The performance gains can be substantial when properly tuned for your use case.
