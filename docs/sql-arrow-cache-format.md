# Apache Arrow Cache Format for Spark

## Overview

Apache Spark supports using Apache Arrow as an alternative cache format for in-memory Dataset caching. This format provides improved performance for certain workloads, especially when working with columnar data sources like Parquet and ORC.

## Benefits

The Arrow cache format offers several advantages over the default cache format:

- **Zero-copy reads** when input is already in Arrow format (e.g., Arrow-based data sources, re-caching Arrow cached data)
- **Better filter pushdown** with min/max statistics for partition pruning
- **Off-heap memory management** via Arrow allocators
- **Efficient compression** with zstd and lz4 codecs
- **Arrow ecosystem interoperability** for data sharing

**Note**: Spark's built-in Parquet/ORC readers use internal column vectors (`OnHeapColumnVector`/`OffHeapColumnVector`), not Arrow format, so they don't benefit from zero-copy optimization.

## Configuration

To enable Arrow cache format, set the static configuration:

```scala
spark.conf.set("spark.sql.cache.serializer",
  "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
```

**Note**: This is a static configuration that must be set before the SparkSession is created.

```scala
val spark = SparkSession.builder()
  .appName("MyApp")
  .config("spark.sql.cache.serializer",
    "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
  .getOrCreate()
```

## Usage

Once configured, use cache operations as normal:

```scala
// Cache a DataFrame
val df = spark.read.parquet("data.parquet")
df.cache()

// Use cached data
df.filter("age > 30").count()

// Uncache when done
df.unpersist()
```

## Compression

Arrow cache supports multiple compression codecs. Configure compression with:

```scala
spark.conf.set("spark.sql.arrow.compression.codec", "zstd")
```

Available options:
- `none` - No compression (fastest, largest size)
- `lz4` - LZ4 compression (fast, good compression)
- `zstd` - Zstandard compression (slower, best compression, **default**)

For zstd, you can also configure the compression level:

```scala
spark.conf.set("spark.sql.arrow.compression.level", "3")  // Default: 3, Range: 1-22
```

## Vectorized Reader

Enable vectorized reading for better performance with primitive types:

```scala
spark.conf.set("spark.sql.cache.vectorizedReader.enabled", "true")
```

When enabled, cached data is read as columnar batches instead of rows, which can significantly improve performance for columnar operations.

## Performance Characteristics

### When Arrow Cache Performs Best

1. **Filter-Heavy Workloads**: Queries with selective filters benefit from min/max statistics
2. **Columnar Operations**: Aggregations, projections on cached data
3. **Arrow-based Data Sources**: When input is already ArrowColumnVector (Python sources, Arrow-based formats)
4. **Large Datasets**: Off-heap memory management scales better

### When Default Cache May Perform Better

1. **Row-based Operations**: Queries that access many columns per row
2. **Small Datasets**: Overhead of Arrow format may not be worth it
3. **Parquet/ORC Sources**: No zero-copy benefit since they use internal column vectors

### Benchmark Results

Based on benchmarks on Apple M4 Max (OpenJDK 21.0.8):

| Workload | Default Cache | Arrow Cache | Improvement |
|----------|--------------|-------------|-------------|
| Write + Read (5M rows, 3 primitive columns) | 611ms (122.2 ns/row) | 591ms (118.1 ns/row) | **3% faster** |
| Filter with stats (5M rows) | 13ms (2.6 ns/row) | 11ms (2.2 ns/row) | **15% faster** |
| Columnar input from Parquet (2M rows, 3 primitive columns) | 293ms (146.7 ns/row) | 293ms (146.6 ns/row) | **Same** |

**Notes**:
- **Filter improvement** comes from min/max statistics enabling batch skipping
- **Parquet caching** shows no improvement because:
  - Spark's Parquet reader produces `OnHeapColumnVector`/`OffHeapColumnVector`, not `ArrowColumnVector`
  - Zero-copy path does NOT trigger for Parquet/ORC sources
  - Both cache formats must convert from Spark's internal vectors to their respective formats
- **Zero-copy benefits** only apply when input is already `ArrowColumnVector` (e.g., Python Arrow sources, re-caching Arrow cached data)

## Supported Data Types

Arrow cache supports all Spark SQL data types:

### Primitive Types
- BooleanType
- ByteType, ShortType, IntegerType, LongType
- FloatType, DoubleType
- DecimalType (all precision/scale combinations)

### Temporal Types
- DateType
- TimestampType
- TimestampNTZType

### String and Binary
- StringType
- BinaryType

### Complex Types
- ArrayType
- StructType
- MapType
- Nested combinations of the above

## Statistics and Filter Pushdown

Arrow cache automatically collects min/max statistics for the following types:
- All numeric types (Boolean, Byte, Short, Int, Long, Float, Double)
- Date and Timestamp types
- String
- Decimal

These statistics enable partition pruning when filtering:

```scala
val df = spark.range(10000000).cache()

// This filter can skip batches using min/max statistics
df.filter("id > 5000000").count()
```

## Memory Management

Arrow cache uses off-heap memory managed by Apache Arrow allocators. Memory is automatically cleaned up when:
- Tasks complete
- DataFrames are unpersisted
- SparkSession is stopped

You can monitor Arrow memory usage through Spark metrics and the Spark UI.

## Limitations and Considerations

1. **Static Configuration**: Cache serializer must be set before SparkSession creation
2. **Memory Overhead**: Arrow format has small per-batch overhead
3. **Compatibility**: Cannot mix cache formats - recache needed when switching
4. **Compression Trade-off**: Higher compression = lower memory but slower reads

## Migration from Default Cache

To migrate from default cache to Arrow cache:

1. **Stop your SparkSession**
2. **Uncache all DataFrames** (optional but recommended)
3. **Update SparkSession configuration**:
   ```scala
   val spark = SparkSession.builder()
     .config("spark.sql.cache.serializer",
       "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
     .getOrCreate()
   ```
4. **Recache your DataFrames**

**Note**: Existing cached data will be invalidated when changing cache format.

## Troubleshooting

### Out of Memory Errors

If you encounter OOM errors with Arrow cache:

1. Reduce batch size:
   ```scala
   spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "5000")  // Default: 10000
   ```

2. Enable compression:
   ```scala
   spark.conf.set("spark.sql.arrow.compression.codec", "zstd")
   ```

3. Reduce compression level:
   ```scala
   spark.conf.set("spark.sql.arrow.compression.level", "1")
   ```

### Slow Performance

If Arrow cache is slower than expected:

1. Enable vectorized reader:
   ```scala
   spark.conf.set("spark.sql.cache.vectorizedReader.enabled", "true")
   ```

2. Try different compression codec:
   ```scala
   spark.conf.set("spark.sql.arrow.compression.codec", "lz4")  // Faster than zstd
   ```

3. Increase batch size (if memory allows):
   ```scala
   spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "20000")
   ```

## Configuration Reference

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.sql.cache.serializer` | DefaultCachedBatchSerializer | Cache format serializer class |
| `spark.sql.arrow.compression.codec` | `zstd` | Compression codec (none, lz4, zstd) |
| `spark.sql.arrow.compression.level` | `3` | Zstd compression level (1-22) |
| `spark.sql.arrow.maxRecordsPerBatch` | `10000` | Maximum rows per Arrow batch |
| `spark.sql.cache.vectorizedReader.enabled` | `false` | Enable vectorized cache reading |

## Example: Complete Application

```scala
import org.apache.spark.sql.SparkSession

object ArrowCacheExample {
  def main(args: Array[String]): Unit = {
    // Create SparkSession with Arrow cache
    val spark = SparkSession.builder()
      .appName("ArrowCacheExample")
      .master("local[*]")
      .config("spark.sql.cache.serializer",
        "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
      .config("spark.sql.arrow.compression.codec", "zstd")
      .config("spark.sql.cache.vectorizedReader.enabled", "true")
      .getOrCreate()

    try {
      // Read columnar data source
      val df = spark.read.parquet("large_dataset.parquet")

      // Cache with Arrow format
      df.cache()

      // Queries benefit from zero-copy reads and statistics
      val result1 = df.filter("age > 30").select("name", "age").count()
      println(s"Filtered count: $result1")

      val result2 = df.groupBy("country").agg(sum("sales")).collect()
      println(s"Aggregation result: ${result2.mkString(", ")}")

      // Uncache when done
      df.unpersist()

    } finally {
      spark.stop()
    }
  }
}
```

## Best Practices

1. **Use with Columnar Sources**: Maximum benefit with Parquet/ORC
2. **Enable Statistics**: Let Arrow cache collect min/max for filter pushdown
3. **Monitor Memory**: Watch off-heap memory usage in production
4. **Test First**: Benchmark your workload before production deployment
5. **Compression**: Start with `lz4` for balanced performance
6. **Vectorization**: Enable vectorized reader for primitive-heavy workloads

## Further Reading

- [Apache Arrow Project](https://arrow.apache.org/)
- [Spark Caching Documentation](https://spark.apache.org/docs/latest/sql-performance-tuning.html#caching-data-in-memory)
- [Arrow IPC Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format)
