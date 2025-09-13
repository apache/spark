# SPARK-52401 Fix Summary

## Issue Description

**JIRA Issue**: [SPARK-52401](https://issues.apache.org/jira/projects/SPARK/issues/SPARK-52401)

**Problem**: When using `saveAsTable` with `mode="append"` on a DataFrame that references an external Spark table, there was an inconsistency between `.count()` and `.collect()` operations. Specifically:

- After appending new data to the table, `.count()` correctly returned the expected number of rows
- However, `.collect()` returned outdated results (empty list) instead of reflecting the updated table contents

**Root Cause**: The issue was in the cache invalidation mechanism for DataSourceV2 tables. When `saveAsTable` with append mode was executed, the `refreshCache` function in `DataSourceV2Strategy` was calling `recacheByPlan`, which attempted to re-execute the same logical plan. However, the logical plan didn't know that the underlying table data had changed, so it returned the same cached data.

## Technical Details

### The Problem

1. **DataFrame Caching**: When a DataFrame references a table via `spark.table(tableName)`, the logical plan gets cached
2. **Append Operation**: When `saveAsTable` with `mode="append"` is called, it modifies the underlying table data
3. **Cache Invalidation**: The `refreshCache` function was using `recacheByPlan`, which re-executes the same logical plan
4. **Stale Data**: Since the logical plan doesn't reflect the table changes, it returns the same cached data

### The Fix

**File Modified**: `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DataSourceV2Strategy.scala`

**Change**: Modified the `refreshCache` function to use `uncacheQuery` instead of `recacheByPlan`:

```scala
private def refreshCache(r: DataSourceV2Relation)(): Unit = {
  // For append operations, we should invalidate the cache instead of recaching
  // because the underlying table data has changed and we want to read fresh data
  // on the next access. recacheByPlan would re-execute the same logical plan
  // which doesn't reflect the table changes.
  session.sharedState.cacheManager.uncacheQuery(session, r, cascade = true)
}
```

### Why This Fix Works

1. **Cache Invalidation**: `uncacheQuery` removes the cached logical plan from the cache
2. **Fresh Data**: On the next access to the DataFrame, Spark will re-read the table and create a new logical plan
3. **Consistency**: Both `.count()` and `.collect()` will now read from the same fresh data source

## Affected Operations

The fix affects all operations that use the `refreshCache` function, which includes:

- `AppendData` (saveAsTable with append mode)
- `OverwriteByExpression` (saveAsTable with overwrite mode)
- `OverwritePartitionsDynamic`
- `DeleteFromTableWithFilters`
- `DeleteFromTable`
- `ReplaceData`
- `WriteDelta`

All of these operations modify table data and should invalidate the cache rather than recache the same logical plan.

## Testing

### Test Files Created

1. **`sql/core/src/test/scala/org/apache/spark/sql/DataFrameCacheSuite.scala`**: Scala test suite with comprehensive test cases
2. **`test_spark_52401.py`**: Simple Python test script to reproduce and verify the fix
3. **`test_spark_52401_comprehensive.py`**: Comprehensive Python test covering multiple scenarios

### Test Scenarios

1. **Basic Append Test**: Verify that both `.count()` and `.collect()` reflect table updates after append
2. **Multiple Appends Test**: Test multiple consecutive append operations
3. **DataFrame Operations Test**: Test various DataFrame operations (filter, select, groupBy) after table updates
4. **Overwrite Operations Test**: Test overwrite operations to ensure they also work correctly

## Impact

### Positive Impact

1. **Consistency**: `.count()` and `.collect()` now return consistent results
2. **Correctness**: DataFrame operations correctly reflect table updates
3. **User Experience**: Users can rely on DataFrame operations to show current table state
4. **Backward Compatibility**: The fix doesn't break existing functionality

### Performance Considerations

1. **Cache Invalidation**: The fix may cause more cache misses, but this is necessary for correctness
2. **Fresh Reads**: Subsequent DataFrame operations will read fresh data from the table
3. **Overall Impact**: Minimal performance impact, as the alternative was incorrect behavior

## Verification

To verify the fix works:

1. **Before Fix**: 
   - `df.count()` returns 1 (correct)
   - `df.collect()` returns [] (incorrect)

2. **After Fix**:
   - `df.count()` returns 1 (correct)
   - `df.collect()` returns [(1, "foo")] (correct)

## Related Issues

This fix addresses the core issue described in SPARK-52401. Similar cache invalidation issues might exist in other contexts, but this fix specifically targets the DataSourceV2 append operations that were causing the inconsistency between `.count()` and `.collect()` operations.

## Conclusion

The fix is minimal, targeted, and addresses the root cause of the issue. It ensures that DataFrame operations correctly reflect table updates after `saveAsTable` append operations, providing users with consistent and correct behavior. 