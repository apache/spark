# Cache Invalidation V2 Read V1 Write Test Suite

## Overview

This test suite verifies that Spark's `CacheManager` properly invalidates cached DataFrames when using DataSource V2 tables with different write strategies.

## Test Scenarios

### 1. V2 Write Without V1_BATCH_WRITE
- **Setup**: Table with `BATCH_READ` + `BATCH_WRITE` capabilities
- **Behavior**: Regular V2 write path
- **Cache Invalidation**: ✅ Works correctly
- **Reason**: `DataSourceV2Strategy` calls `refreshCache` on the `DataSourceV2Relation`

### 2. V1Write With V1_BATCH_WRITE
- **Setup**: Table with `BATCH_READ` + `V1_BATCH_WRITE` capabilities
- **Behavior**: Writes through V1Write interface
- **Cache Invalidation**: ✅ Works correctly
- **Reason**: `AppendDataExecV1` calls `refreshCache` on the `DataSourceV2Relation`

### 3. Explicit refreshTable
- **Setup**: Any V2 table
- **Behavior**: Explicit call to `spark.catalog.refreshTable()`
- **Cache Invalidation**: ✅ Works correctly (workaround)
- **Reason**: Direct cache invalidation via catalog API

### 4. Comparison Test
- **Setup**: Both write strategies side-by-side
- **Behavior**: Verifies both approaches handle cache correctly
- **Cache Invalidation**: ✅ Both work correctly

## Key Findings

All tested scenarios show that **cache invalidation works correctly** for DataSource V2 tables when:
1. Writes go through `DataSourceV2Strategy` (both regular V2 writes and V1Write)
2. The cache is keyed by `DataSourceV2Relation`
3. The invalidation callback receives the same `DataSourceV2Relation`

## Implementation Details

### HybridV2ReadV1WriteCatalog
- Custom test catalog that creates tables with configurable write capabilities
- Uses in-memory storage via a global `ConcurrentHashMap`
- Supports both `hybrid_no_v1_batch_write` and `hybrid_with_v1_batch_write` providers

### Table Types
- **HybridTableV2Write**: Uses `BATCH_WRITE` capability (regular V2 write)
- **HybridTableV1Write**: Uses `V1_BATCH_WRITE` capability (V1Write interface)

### Data Storage
- Global shared `ConcurrentHashMap` for data storage
- Accessible from both driver and executors
- Thread-safe for concurrent reads and writes

## Running the Tests

```bash
cd /Users/vitalii.li/spark
build/sbt "sql/Test/testOnly *CacheInvalidationV2ReadV1WriteSuite"
```

## Test Results

✅ All 4 tests pass successfully:
- V2 write without V1_BATCH_WRITE properly invalidates cache
- V1Write with V1_BATCH_WRITE properly invalidates cache
- refreshTable explicitly invalidates cache
- Both write strategies properly invalidate cache

## Conclusion

The test suite demonstrates that Spark's cache invalidation mechanism works correctly for DataSource V2 tables regardless of whether they use regular V2 writes or fall back to V1Write. The key is that all write operations go through `DataSourceV2Strategy` which ensures proper cache invalidation.

