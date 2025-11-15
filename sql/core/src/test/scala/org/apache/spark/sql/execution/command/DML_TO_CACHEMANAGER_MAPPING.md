# DML Operations → CacheManager Methods Mapping

## 1. INSERT Operations

### 1.1 SQL INSERT INTO (V2 Table with BATCH_WRITE)
**Operation**: `INSERT INTO v2_table VALUES (...)`
**Execution Path**: 
- `DataSourceV2Strategy` → `AppendDataExec`
- Calls: **`refreshCache()`** callback → `cacheManager.recacheByPlan(session, DataSourceV2Relation)`
**CacheManager Method**: `recacheByPlan(spark, DataSourceV2Relation)`
**Location**: DataSourceV2Strategy.scala:64-66

### 1.2 SQL INSERT INTO (V2 Table with V1_BATCH_WRITE)
**Operation**: `INSERT INTO v2_table VALUES (...)`
**Execution Path**:
- `DataSourceV2Strategy` → `AppendDataExecV1`
- Calls: **`refreshCache()`** callback → `cacheManager.recacheByPlan(session, DataSourceV2Relation)`
**CacheManager Method**: `recacheByPlan(spark, DataSourceV2Relation)`
**Location**: DataSourceV2Strategy.scala:267-272

### 1.3 SQL INSERT INTO (V1 File-based Table: Parquet, ORC, etc.)
**Operation**: `INSERT INTO parquet_table VALUES (...)`
**Execution Path**:
- `DataSourceStrategy` → `InsertIntoHadoopFsRelationCommand`
- Line 212: `sparkSession.sharedState.cacheManager.recacheByPath(sparkSession, outputPath, fs)`
**CacheManager Method**: `recacheByPath(spark, outputPath, fs)`
**Cache Invalidation Issue**: ⚠️ **Only works for a LogicalRelation with same path or FileTable**
**Location**: InsertIntoHadoopFsRelationCommand.scala:212

### 1.4 SQL INSERT INTO (V1 InsertableRelation)
**Operation**: `INSERT INTO insertable_table VALUES (...)`
**Execution Path**:
- `DataSourceStrategy` → `InsertIntoDataSourceCommand`
- Line 48: `sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, logicalRelation)`
**Cache Invalidation Issue**: ⚠️ **Does not work if read uses V2**
**CacheManager Method**: `recacheByPlan(spark, LogicalRelation)`
**Location**: InsertIntoDataSourceCommand.scala:48

### 1.5 SQL INSERT INTO (Hive Table)
**Operation**: `INSERT INTO hive_table VALUES (...)`
**Execution Path**:
- `HiveAnalysis` → `InsertIntoHiveTable`
- Lines 112-114:
  1. `CommandUtils.uncacheTableOrView(sparkSession, catalogTable)`
  2. `sparkSession.sessionState.catalog.refreshTable(tableIdentifier)`
- `refreshTable()` then calls: `cacheManager.recacheByPlan(sparkSession, plan)`
**CacheManager Method**: 
- `uncacheTableOrView()` → internally calls `uncacheByCondition()`
- Then `recacheByPlan(spark, plan)` via catalog.refreshTable()
**Location**: InsertIntoHiveTable.scala:112-114

### 1.6 DataFrame.write.insertInto("table")
**Operation**: `df.write.insertInto("table_name")`
**Execution Path**:
- Creates `InsertIntoStatement` → follows one of the above paths based on table type
- **Same as SQL INSERT** (1.1, 1.2, 1.3, 1.4, or 1.5)
**CacheManager Method**: Depends on table type (see above)
**Location**: DataFrameWriter.scala:304-308

### 1.7 DataFrame.write.save(path) or DataFrame.write.format().save()
**Operation**: `df.write.parquet(path)` or `df.write.format("parquet").save(path)`
**Execution Path**:
- `DataFrameWriter.saveCommand()` → `saveToV1SourceCommand()`
- For V1 sources: `DataSource.planForWriting()` → `SaveIntoDataSourceCommand`
- Line 75: `sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, logicalRelation)`
**CacheManager Method**: `recacheByPlan(spark, LogicalRelation)`
**Cache Invalidation Issue**: ⚠️ **Only works if a LogicalRelation with same path exists in cache**
**Cache Invalidation Issue**: ⚠️ **Only works if for a LogicalRelation or FileTable**
**Location**: SaveIntoDataSourceCommand.scala:75

### 1.8 DataFrame.write.saveAsTable("table")
**Operation**: `df.write.saveAsTable("table_name")`
**Execution Path**:
- If table exists: Creates `AppendData` or `OverwriteByExpression` → follows path 1.1 or 1.2
- If table doesn't exist: Creates `CreateTableAsSelect` → no cache to invalidate
**CacheManager Method**: 
- Existing table: `recacheByPlan(spark, DataSourceV2Relation)` or path-based
- New table: N/A
**Cache Invalidation Issue**: ⚠️ **Only works if a LogicalRelation with same path exists in cache**
**Cache Invalidation Issue**: ⚠️ **Only works if for a LogicalRelation or FileTable**
**Location**: DataFrameWriter.scala:430-511

## 2. UPDATE/DELETE/MERGE Operations

Data source dependent. E.g. Delta calls `recacheByPlan(sparkSession, logicalRelation)`
**Cache Invalidation Issue**: ⚠️ **Does not work if read uses V2**


## 5. TRUNCATE Operations

### 5.1 SQL TRUNCATE TABLE (V2 Table)
**Operation**: `TRUNCATE TABLE table_name`
**Execution Path**:
- `DataSourceV2Strategy` → `TruncateTableExec`
- Calls: **`refreshCache()`** callback → `cacheManager.recacheByPlan(session, DataSourceV2Relation)`
**CacheManager Method**: `recacheByPlan(spark, DataSourceV2Relation)`

### 5.2 SQL TRUNCATE TABLE (V1 Table)
**Operation**: `TRUNCATE TABLE table_name`
**Execution Path**:
- Calls: `CommandUtils.uncacheTableOrView()` + `catalog.refreshTable()`
**CacheManager Method**: 
- `uncacheTableOrView()`
- Then `recacheByPlan()` via refreshTable

## 6. OVERWRITE Operations

### 6.1 SQL INSERT OVERWRITE (V2 Table)
**Operation**: `INSERT OVERWRITE TABLE v2_table VALUES (...)`
**Execution Path**:
- `DataSourceV2Strategy` → `OverwriteByExpressionExec` or `OverwritePartitionsDynamicExec`
- Calls: **`refreshCache()`** callback → `cacheManager.recacheByPlan(session, DataSourceV2Relation)`
**CacheManager Method**: `recacheByPlan(spark, DataSourceV2Relation)`

### 6.2 SQL INSERT OVERWRITE (V2 Table with V1_BATCH_WRITE)
**Operation**: `INSERT OVERWRITE TABLE v2_table VALUES (...)`
**Execution Path**:
- `DataSourceV2Strategy` → `OverwriteByExpressionExecV1`
- Line 286: `refreshCache` callback → `cacheManager.recacheByPlan(session, DataSourceV2Relation)`
**CacheManager Method**: `recacheByPlan(spark, DataSourceV2Relation)`
**Location**: DataSourceV2Strategy.scala:281-289

### 6.3 SQL INSERT OVERWRITE (V1 File-based Table)
**Operation**: `INSERT OVERWRITE TABLE parquet_table VALUES (...)`
**Execution Path**:
- Same as 1.3: `InsertIntoHadoopFsRelationCommand`
**CacheManager Method**: `recacheByPath(spark, outputPath, fs)`
**Cache Invalidation Issue**: ⚠️ **Only works if a LogicalRelation with same path exists in cache**
**Cache Invalidation Issue**: ⚠️ **Only works if for a LogicalRelation or FileTable**

### 6.4 DataFrame.write.mode("overwrite").save(path)
**Operation**: `df.write.mode("overwrite").parquet(path)`
**Execution Path**:
- Same as 1.7: `SaveIntoDataSourceCommand`
**CacheManager Method**: `recacheByPlan(spark, LogicalRelation)`
**Cache Invalidation Issue**: ⚠️ **Same issue as 1.7**

## 7. Manual Cache Operations

### 7.1 SQL REFRESH TABLE
**Operation**: `REFRESH TABLE table_name`
**Execution Path**:
- `DataSourceV2Strategy` → `RefreshTableExec`
- Calls: `recacheTable(r)` → `cacheManager.recacheByPlan(spark, r.plan)`
**CacheManager Method**: `recacheByPlan(spark, plan)`
**Location**: DataSourceV2Strategy.scala:219

### 7.2 Catalog API: spark.catalog.refreshTable()
**Operation**: `spark.catalog.refreshTable("table_name")`
**Execution Path**:
- `Catalog.refreshTable()` → uncache + `cacheManager.recacheByPlan()`
**CacheManager Method**: 
- `uncacheQuery()`
- Then `recacheByPlan(spark, plan)`
**Location**: Catalog.scala:870-895

### 7.3 Catalog API: spark.catalog.clearCache()
**Operation**: `spark.catalog.clearCache()`
**Execution Path**:
- Direct call
**CacheManager Method**: `clearCache()`

## Summary of CacheManager Methods Used

### `recacheByPlan(spark: SparkSession, plan: LogicalPlan)`
**Issues for**:
- All DML operations (INSERT, UPDATE, DELETE, MERGE, TRUNCATE, OVERWRITE)
- V1 InsertableRelation (INSERT)
- V1 SaveIntoDataSourceCommand (DataFrame.write.save)

**How it works**: 
- Normalizes the plan
- Finds cache entries where any plan node has `sameResult(normalized)` = true
- Clears and rebuilds those cache entries
- **Key**: Uses `sameResult()` for matching, so plan types must match

### `recacheByPath(spark: SparkSession, path: Path, fs: FileSystem)`
**Used by**:
- V1 file-based operations (INSERT/OVERWRITE with Parquet, ORC, etc.)
- InsertIntoHadoopFsRelationCommand

**How it works**:
- Finds cache entries containing `LogicalRelation` with `HadoopFsRelation` matching the path
- OR finds cache entries containing `FileTable` matching the path
- Refreshes file index and rebuilds cache
- **Key**: Only matches `LogicalRelation` or `FileTable`, NOT `DataSourceV2Relation`

### `uncacheTableOrView(spark: SparkSession, name: Seq[String], cascade: Boolean)`
**Used by**:
- Hive INSERT/UPDATE/DELETE operations (before refreshTable)
- DROP TABLE/VIEW operations
- ALTER TABLE operations

**How it works**:
- Removes cache entries by table/view name
- Optionally cascades to dependent queries

