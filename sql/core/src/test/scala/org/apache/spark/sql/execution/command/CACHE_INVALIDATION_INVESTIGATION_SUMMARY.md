# Cache Invalidation Investigation: V2 Read + V1 Write Paths

## Summary of Investigation

After investigating Spark's cache invalidation mechanisms, we found that **cache invalidation works correctly** in all standard V2 table write scenarios.

## Key Findings

### When Cache Invalidation Works ✅

Cache invalidation works correctly for DataSource V2 tables when:

1. **V2 Writes (BATCH_WRITE capability)**
   - Write goes through `DataSourceV2Strategy`
   - Calls `refreshCache(session, r)` where `r` is `DataSourceV2Relation`
   - Cache is keyed by `DataSourceV2Relation`, invalidation matches correctly

2. **V1 Writes with V1_BATCH_WRITE capability**
   - Write goes through `AppendDataExecV1` or `OverwriteByExpressionExecV1`
   - These also call `refreshCache(session, r)` with `DataSourceV2Relation`
   - Cache invalidation works correctly

3. **Explicit `refreshTable()`**
   - Always invalidates cache via catalog API
   - Works as a manual workaround

### Theoretical Bug Scenario ❌

The ONLY scenario where cache invalidation could fail is:

1. **Table is cached via V2 read** → Creates `DataSourceV2Relation` in cache
2. **Write goes through pure V1 path** → Uses `InsertIntoDataSourceCommand` or `InsertIntoHadoopFsRelationCommand`
3. **V1 commands call**:
   - `recacheByPlan(sparkSession, logicalRelation)` with `LogicalRelation`, OR
   - `recacheByPath(sparkSession, outputPath, fs)` which looks for `LogicalRelation` or `FileTable`
4. **Cache key mismatch** → Cache has `DataSourceV2Relation`, invalidation looks for `LogicalRelation`/`FileTable`
5. **Result**: Cache becomes stale

### Why This Doesn't Happen in Practice

**Modern Spark routing prevents this scenario:**

- If a table is in a V2 catalog and supports reads, Spark creates `DataSourceV2Relation` for reads
- For writes:
  - If table has `BATCH_WRITE` → Uses V2 write path with correct cache invalidation
  - If table has `V1_BATCH_WRITE` → Uses `V1Write` but still calls V2 `refreshCache`
  - If table has NO write capabilities → INSERT is rejected
  - **FileTable fallback** (`FallBackFileSourceV2`) converts the entire `InsertIntoStatement` to use `LogicalRelation`, so reads and writes are consistent

**The FileTable case is special:**
- `FallBackFileSourceV2` rule converts INSERT into FileTable to use `LogicalRelation`
- But it does this BEFORE the table is read/cached
- So if you cache the table, it's already cached as `LogicalRelation`
- Write uses `InsertIntoHadoopFsRelationCommand` which calls `recacheByPath`
- `recacheByPath` looks for `LogicalRelation` or `FileTable` - matches correctly!

## Conclusion

**There is NO practical bug in Spark's cache invalidation for V2 tables.**

The theoretical bug scenario (V2 cached relation + V1 write path without proper invalidation) cannot occur in practice because:
1. Spark's query planning ensures consistency between read and write paths
2. V2 write operations always trigger proper cache invalidation callbacks
3. FileTable fallback happens early enough to maintain consistency

The cache invalidation mechanism is **working as designed**.

## Implementation Notes

### CacheManager Methods

- **`recacheByPlan(plan)`**: Invalidates cache entries that match the given plan via `sameResult()`
  - Works when cache key and invalidation key are the same type (both `DataSourceV2Relation` or both `LogicalRelation`)

- **`recacheByPath(path)`**: Invalidates cache entries containing `LogicalRelation` or `FileTable` with matching path
  - Used by V1 file-based write commands
  - Won't match `DataSourceV2Relation` (but this is OK because FileTable fallback ensures consistency)

- **`refreshTable()`**: Direct invalidation via catalog API
  - Always works as a workaround for any edge cases

### Key Code Paths

1. **V2 Write**: `DataSourceV2Strategy` → `AppendDataExec`/`OverwriteByExpressionExec` → `refreshCache(DataSourceV2Relation)`
2. **V1Write**: `DataSourceV2Strategy` → `AppendDataExecV1`/`OverwriteByExpressionExecV1` → `refreshCache(DataSourceV2Relation)`
3. **FileTable**: `FallBackFileSourceV2` → `InsertIntoHadoopFsRelationCommand` → `recacheByPath()` → matches `LogicalRelation`

All paths maintain consistency between cache keys and invalidation mechanisms.

