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
It selects the cache serializer for the whole session; once set, this serializer handles every
cached relation. There is no automatic per-relation fallback to another cache serializer based on
the data types involved (see [Supported Data Types](#supported-data-types) for how unsupported
types are handled).

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
spark.conf.set("spark.sql.execution.arrow.compression.codec", "zstd")
```

Available options:
- `none` - No compression (fastest, largest size, **default**)
- `lz4` - LZ4 compression (fast, good compression)
- `zstd` - Zstandard compression (slower, best compression)

For zstd, you can also configure the compression level. Positive values (up to 22) give better
compression but slower speed; negative values give ultra-fast compression with lower ratios:

```scala
spark.conf.set("spark.sql.execution.arrow.compression.zstd.level", "3")  // Default: 3
```

## Vectorized Reader

Enable vectorized reading for better performance with primitive types:

```scala
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
```

When enabled, cached data is read as columnar batches instead of rows, which can significantly improve performance for columnar operations.

## Performance Characteristics

In our benchmarks, the Arrow cache format performs best on the following workloads. Actual
results depend on data types, compression settings, and hardware, and the default cache format
can be faster in some cases (for example, with higher compression levels):

1. **Filter-Heavy Workloads**: Queries with selective filters benefit from min/max statistics.
2. **Columnar Operations**: Aggregations and projections on cached data benefit from the Arrow format.
3. **Parquet/ORC Caching**: Arrow's batch processing helps even without the zero-copy path.
4. **Re-caching with Column Projection**: Dropping columns from Arrow-cached data preserves the
   `ArrowColumnVector` format, enabling true zero-copy extraction and the largest gains.

### Benchmark Results

The numbers below are illustrative results from one run on an Apple M4 Max (OpenJDK 21.0.8) and
will vary with hardware, JDK, and compression settings. They are not a guarantee. For the
authoritative, regularly regenerated numbers, see
`sql/core/benchmarks/ArrowCacheBenchmark-jdk21-results.txt` and the `ArrowCacheBenchmark` suite.

| Workload | Default Cache | Arrow Cache | Speedup |
|----------|--------------|-------------|---------|
| Write + Read (5M rows, 3 primitive columns) | 153.7 ns/row | 74.2 ns/row | **~2X faster** |
| Filter with stats (5M rows) | 100.1 ns/row | 70.8 ns/row | **~1.4X faster** |
| Columnar input from Parquet (2M rows, 3 primitive columns) | 195.3 ns/row | 113.1 ns/row | **~1.7X faster** |
| Re-cache with zero-copy (2M rows, 2 columns) | 123.3 ns/row | 38.5 ns/row | **~3.2X faster** |

**Notes**:
- **Write + Read**: Significant improvement from efficient Arrow serialization and vectorized operations
- **Filter improvement**: Comes from min/max statistics enabling batch skipping during partition pruning
- **Parquet caching**: Shows improvement despite Spark's Parquet reader producing `OnHeapColumnVector`/`OffHeapColumnVector` rather than `ArrowColumnVector`, due to Arrow's efficient batch processing
- **Re-cache with zero-copy**: When caching a subset of columns from Arrow-cached data (e.g., `df.drop("column")`), the remaining columns preserve their `ArrowColumnVector` format, enabling true zero-copy extraction and achieving the best performance
- **Zero-copy benefits** only apply when input is already `ArrowColumnVector` (e.g., Python Arrow sources, re-caching Arrow cached data with column projection)

## Supported Data Types

Arrow cache supports the following data types:

### Primitive Types
- BooleanType
- ByteType, ShortType, IntegerType, LongType
- FloatType, DoubleType
- DecimalType (all precision/scale combinations)
- NullType

### Temporal Types
- DateType
- TimestampType
- TimestampNTZType
- TimeType

### Interval Types
- YearMonthIntervalType
- DayTimeIntervalType
- CalendarIntervalType

### String and Binary
- StringType (including collated strings)
- BinaryType

### Complex Types
- ArrayType
- StructType
- MapType
- Nested combinations of the above

### Other Types
- VariantType
- GeometryType, GeographyType
- User-defined types (UDTs) whose underlying representation is itself supported

### Unsupported Types

Arrow cache covers every type the default cache serializer supports, plus some it
does not (for example geometry and geography). Types that Arrow cannot represent
(such as `ObjectType`) are not silently dropped or routed to a different cache
serializer: there is no per-type fallback, because the cache serializer is chosen
once via the static `spark.sql.cache.serializer` configuration and then handles
every cached relation. Attempting to cache an unsupported type fails with an
`UNSUPPORTED_DATATYPE` error when the cache is materialized.

## Statistics and Filter Pushdown

Arrow cache automatically collects min/max statistics for the following types:
- Boolean
- Numeric types (Byte, Short, Int, Long, Float, Double)
- Decimal
- Date, Timestamp, and Timestamp without time zone (TIMESTAMP_NTZ)
- Time
- Year-month and day-time intervals
- String (using collation-aware comparison for collated strings)

Other types (Binary, Variant, calendar intervals, and complex types such as
Array/Struct/Map) are cached but do not contribute min/max bounds, so they only
record null counts and sizes.

These statistics enable partition pruning when filtering:

```scala
val df = spark.range(10000000).cache()

// This filter can skip batches using min/max statistics
df.filter("id > 5000000").count()
```

## Memory Management

Arrow cache uses off-heap memory managed by Apache Arrow allocators. This is a fundamental design choice in Apache Arrow and is not configurable for on-heap memory.

**Memory Efficiency**:
- Despite requiring off-heap memory, Arrow cache is often **more memory-efficient** than default cache:
  - Efficient compression with zstd/lz4 codecs
  - Compact columnar format without Java object overhead
  - Better compression ratios, especially for strings and complex types
- If you have limited off-heap memory, increase `spark.executor.memoryOverhead` to allocate more off-heap memory

**Memory Cleanup**:
Arrow memory is automatically cleaned up when:
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
   spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")  // Default: 10000
   ```

2. Enable compression:
   ```scala
   spark.conf.set("spark.sql.execution.arrow.compression.codec", "zstd")
   ```

3. Reduce compression level:
   ```scala
   spark.conf.set("spark.sql.execution.arrow.compression.zstd.level", "1")
   ```

### Slow Performance

If Arrow cache is slower than expected:

1. Enable vectorized reader:
   ```scala
   spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
   ```

2. Try different compression codec:
   ```scala
   spark.conf.set("spark.sql.execution.arrow.compression.codec", "lz4")  // Faster than zstd
   ```

3. Increase batch size (if memory allows):
   ```scala
   spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
   ```

## Configuration Reference

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.sql.cache.serializer` | DefaultCachedBatchSerializer | Cache format serializer class |
| `spark.sql.execution.arrow.compression.codec` | `none` | Compression codec (none, lz4, zstd) |
| `spark.sql.execution.arrow.compression.zstd.level` | `3` | Zstd compression level (negative = faster, up to 22) |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | `10000` | Maximum rows per Arrow batch |
| `spark.sql.inMemoryColumnarStorage.enableVectorizedReader` | `true` | Enable vectorized cache reading |

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
      .config("spark.sql.execution.arrow.compression.codec", "zstd")
      .config("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
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
