# Migration Guide: Default Cache to Arrow Cache Format

## Overview

This guide helps you migrate your Spark applications from the default cache format to the Apache Arrow cache format safely and effectively.

## Prerequisites

- Apache Spark 4.0.0 or later
- Basic understanding of Spark caching mechanisms
- Access to modify SparkSession configuration

## Migration Checklist

- [ ] Review workload characteristics
- [ ] Benchmark current performance
- [ ] Test Arrow cache in development
- [ ] Monitor memory usage
- [ ] Validate results correctness
- [ ] Deploy to staging
- [ ] Monitor production metrics
- [ ] Rollback plan ready

## Step-by-Step Migration

### Step 1: Assess Your Workload

Arrow cache performs best with certain workload characteristics. Evaluate your use case:

**Good Candidates** ✅:
- Reads from Parquet, ORC, or columnar formats
- Filter-heavy queries (WHERE clauses)
- Columnar aggregations (GROUP BY, SUM, AVG)
- Large cached datasets (> 1GB)
- Repeated reads from cached data

**Consider Carefully** ⚠️:
- Row-oriented operations
- Small datasets (< 100MB)
- Frequent cache/uncache cycles
- Limited off-heap memory

### Step 2: Benchmark Current Performance

Before migrating, establish baseline metrics:

```scala
// Current performance with default cache
val df = spark.read.parquet("data.parquet")

val startCache = System.currentTimeMillis()
df.cache()
df.count()
val cacheTime = System.currentTimeMillis() - startCache
println(s"Cache time: ${cacheTime}ms")

val startQuery = System.currentTimeMillis()
val result = df.filter("age > 30").count()
val queryTime = System.currentTimeMillis() - startQuery
println(s"Query time: ${queryTime}ms")
println(s"Result: $result")

df.unpersist()
```

Record these baseline metrics for comparison.

### Step 3: Create Test Environment

Set up a separate test environment with Arrow cache:

```scala
val sparkArrow = SparkSession.builder()
  .appName("ArrowCacheTest")
  .master("local[*]")
  .config("spark.sql.cache.serializer",
    "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
  .config("spark.sql.arrow.compression.codec", "lz4")  // Start with lz4
  .config("spark.sql.cache.vectorizedReader.enabled", "true")
  .getOrCreate()
```

### Step 4: Run Parallel Tests

Test Arrow cache with the same workload:

```scala
val df = sparkArrow.read.parquet("data.parquet")

val startCache = System.currentTimeMillis()
df.cache()
df.count()
val cacheTime = System.currentTimeMillis() - startCache
println(s"Arrow cache time: ${cacheTime}ms")

val startQuery = System.currentTimeMillis()
val result = df.filter("age > 30").count()
val queryTime = System.currentTimeMillis() - startQuery
println(s"Arrow query time: ${queryTime}ms")
println(s"Result: $result")  // Verify same result!

df.unpersist()
```

### Step 5: Validate Correctness

**Critical**: Ensure results match exactly:

```scala
// Compare results
val defaultResult = sparkDefault.read.parquet("data.parquet")
  .cache()
  .filter("age > 30")
  .select("name", "age", "salary")
  .collect()

val arrowResult = sparkArrow.read.parquet("data.parquet")
  .cache()
  .filter("age > 30")
  .select("name", "age", "salary")
  .collect()

assert(defaultResult.sameElements(arrowResult),
  "Results differ between cache formats!")
```

### Step 6: Tune Configuration

Optimize Arrow cache configuration based on your workload:

#### For Memory-Constrained Environments

```scala
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "5000")  // Smaller batches
spark.conf.set("spark.sql.arrow.compression.codec", "zstd")   // Better compression
spark.conf.set("spark.sql.arrow.compression.level", "5")      // Higher compression
```

#### For Performance-Critical Applications

```scala
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "20000")  // Larger batches
spark.conf.set("spark.sql.arrow.compression.codec", "lz4")     // Faster codec
spark.conf.set("spark.sql.cache.vectorizedReader.enabled", "true")
```

#### For Balanced Configuration

```scala
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "10000")  // Default
spark.conf.set("spark.sql.arrow.compression.codec", "zstd")
spark.conf.set("spark.sql.arrow.compression.level", "3")       // Default
spark.conf.set("spark.sql.cache.vectorizedReader.enabled", "true")
```

### Step 7: Monitor Memory Usage

Track memory metrics during testing:

```scala
import org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer

// Monitor cache size
val cachedTables = spark.sharedState.cacheManager.lookupCachedData(df.logicalPlan)
cachedTables.foreach { cached =>
  val sizeInBytes = cached.cachedRepresentation.sizeInBytesStats.value
  println(s"Cache size: ${sizeInBytes / (1024 * 1024)}MB")
}
```

### Step 8: Production Deployment

#### Option A: Gradual Rollout (Recommended)

Deploy to a subset of applications first:

1. **Week 1**: Deploy to 10% of applications
2. **Week 2**: Monitor metrics, expand to 30%
3. **Week 3**: Expand to 60% if stable
4. **Week 4**: Full rollout

#### Option B: A/B Testing

Run both cache formats side-by-side:

```scala
// Split workload
if (appConfig.useArrowCache) {
  sparkConf.set("spark.sql.cache.serializer",
    "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
}
```

### Step 9: Rollback Plan

Always have a rollback strategy:

```scala
// Quick rollback: Remove Arrow cache configuration
val spark = SparkSession.builder()
  .appName("MyApp")
  // .config("spark.sql.cache.serializer", "...ArrowCachedBatchSerializer")  // Commented out
  .getOrCreate()
```

Or use feature flags:

```scala
val cacheSerializer = if (config.enableArrowCache) {
  "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer"
} else {
  "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer"
}

spark.conf.set("spark.sql.cache.serializer", cacheSerializer)
```

## Common Migration Patterns

### Pattern 1: Batch Processing Pipeline

**Before**:
```scala
val spark = SparkSession.builder()
  .appName("BatchJob")
  .getOrCreate()

val df = spark.read.parquet("input/*.parquet")
df.cache()

// Multiple transformations using cached data
val result1 = df.filter("status = 'active'").count()
val result2 = df.groupBy("category").agg(sum("amount"))

df.unpersist()
```

**After**:
```scala
val spark = SparkSession.builder()
  .appName("BatchJob")
  .config("spark.sql.cache.serializer",
    "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
  .config("spark.sql.arrow.compression.codec", "lz4")
  .getOrCreate()

val df = spark.read.parquet("input/*.parquet")
df.cache()  // Now uses Arrow format

// Same transformations, better performance
val result1 = df.filter("status = 'active'").count()  // Benefits from statistics
val result2 = df.groupBy("category").agg(sum("amount"))  // Vectorized execution

df.unpersist()
```

### Pattern 2: Interactive Queries

**Before**:
```scala
val cachedData = spark.read.parquet("large_dataset.parquet").cache()

// Multiple users running queries
cachedData.filter("region = 'US'").show()
cachedData.filter("age > 30").show()
cachedData.groupBy("product").count().show()
```

**After**:
```scala
// Configure Arrow cache with vectorization
spark.conf.set("spark.sql.cache.vectorizedReader.enabled", "true")

val cachedData = spark.read.parquet("large_dataset.parquet").cache()

// Same queries, improved filter pushdown
cachedData.filter("region = 'US'").show()      // Uses statistics
cachedData.filter("age > 30").show()           // Uses statistics
cachedData.groupBy("product").count().show()    // Vectorized
```

### Pattern 3: Streaming with Cached Lookups

**Before**:
```scala
val lookupData = spark.read.parquet("lookup.parquet").cache()

spark.readStream
  .format("kafka")
  .load()
  .join(lookupData, "id")  // Uses cached lookup
  .writeStream
  .start()
```

**After**:
```scala
// Arrow cache for lookup table
val lookupData = spark.read.parquet("lookup.parquet").cache()

spark.readStream
  .format("kafka")
  .load()
  .join(lookupData, "id")  // Arrow cache with zero-copy reads
  .writeStream
  .start()
```

## Performance Comparison Matrix

| Workload Type | Default Cache | Arrow Cache | Recommendation |
|---------------|---------------|-------------|----------------|
| Parquet scans + cache | Baseline | +5-10% faster | ✅ Use Arrow |
| Filter-heavy queries | Baseline | +10-15% faster | ✅ Use Arrow |
| Full table scans | Baseline | ~Same | Either OK |
| Row-by-row access | Baseline | -5% slower | ⚠️ Use Default |
| Small datasets (<100MB) | Baseline | ~Same | Either OK |
| Large datasets (>10GB) | Baseline | +5-10% faster | ✅ Use Arrow |

## Troubleshooting Migration Issues

### Issue 1: OOM with Arrow Cache

**Symptom**: Out of memory errors after switching to Arrow cache

**Solution**:
```scala
// Reduce batch size
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "5000")

// Increase compression
spark.conf.set("spark.sql.arrow.compression.codec", "zstd")
spark.conf.set("spark.sql.arrow.compression.level", "5")
```

### Issue 2: Slower Performance

**Symptom**: Queries are slower with Arrow cache

**Solution**:
```scala
// Enable vectorization
spark.conf.set("spark.sql.cache.vectorizedReader.enabled", "true")

// Use faster compression
spark.conf.set("spark.sql.arrow.compression.codec", "lz4")

// Increase batch size (if memory allows)
spark.conf.set("spark.sql.arrow.maxRecordsPerBatch", "20000")
```

### Issue 3: Incorrect Results

**Symptom**: Results differ between cache formats

**This should never happen!** If you encounter this:

1. File a bug report with reproduction steps
2. Rollback to default cache immediately
3. Provide schema and query details

### Issue 4: Cache Not Being Used

**Symptom**: Physical plan doesn't show InMemoryTableScan

**Solution**:
```scala
// Verify cache is materialized
df.cache()
df.count()  // Forces cache materialization

// Check physical plan
df.filter("age > 30").explain()
// Should show: InMemoryTableScan
```

## Monitoring and Metrics

### Key Metrics to Track

1. **Cache Hit Rate**: Should remain constant
2. **Query Latency**: Should improve for filter-heavy queries
3. **Memory Usage**: May differ slightly
4. **Cache Size**: Compare compressed sizes

### Monitoring Code

```scala
def monitorCache(df: DataFrame): Unit = {
  val plan = df.queryExecution.optimizedPlan
  val cached = spark.sharedState.cacheManager.lookupCachedData(plan)

  cached.foreach { c =>
    val stats = c.cachedRepresentation.sizeInBytesStats
    println(s"Cache size: ${stats.value / (1024 * 1024)}MB")
    println(s"Cached partitions: ${c.cachedRepresentation.cacheBuilder.cachedColumnBuffers.getNumPartitions}")
  }
}
```

## Post-Migration Validation

After migration, validate:

- [ ] All tests pass
- [ ] Performance meets expectations
- [ ] Memory usage is acceptable
- [ ] No correctness issues
- [ ] Monitoring dashboards updated
- [ ] Documentation updated
- [ ] Team trained on new format

## Getting Help

If you encounter issues during migration:

1. Check logs for Arrow-related exceptions
2. Review configuration settings
3. Test with smaller datasets first
4. Consult the main documentation: `docs/sql-arrow-cache-format.md`
5. File issues on Apache Spark JIRA

## Conclusion

Arrow cache migration is straightforward for most workloads. Follow this guide, test thoroughly, and deploy gradually for a smooth transition.
