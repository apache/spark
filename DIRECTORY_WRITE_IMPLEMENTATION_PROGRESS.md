# INSERT OVERWRITE DIRECTORY Implementation Progress

## Branch
- **Feature Branch**: `feature/directory-write-support`
- **Base**: Apache Spark 4.2.0-SNAPSHOT (master branch)
- **Repository**: `/Users/a.rana/Projects/spark-directory-write-feature`

## Implementation Checklist

### ✅ Step 1: Protobuf Changes (COMPLETED)
**Files Modified:**
- `sql/connect/common/src/main/protobuf/spark/connect/pipelines.proto`

**Changes:**
1. ✅ Added `DIRECTORY = 5` to `OutputType` enum (line ~283)
2. ✅ Added `DirectoryDetails directory_details = 8` to `DefineOutput.details` oneof (line ~90)
3. ✅ Added `DirectoryDetails` message with fields:
   - `path`: Output directory path (S3, HDFS, local)
   - `format`: File format (parquet, orc, csv, json)
   - `mode`: Write mode (overwrite, append, etc.)
   - `options`: Additional write options (compression, partitionBy, etc.)

**Testing:** Will need to rebuild protobuf files with `mvn clean install -DskipTests -pl :spark-connect-common_2.13`

---

### ⏳ Step 2: Add Directory Graph Element (TODO)
**Files to Create/Modify:**
- `sql/pipelines/src/main/scala/org/apache/spark/sql/pipelines/graph/GraphElements.scala`

**Changes Needed:**
1. Add `Directory` case class extending `Output`
2. Add `directories: Seq[Directory]` to `DataflowGraph`
3. Add helper methods for directory management

---

### ⏳ Step 3: SQL Handler for INSERT INTO DIR (TODO)
**Files to Modify:**
- `sql/pipelines/src/main/scala/org/apache/spark/sql/pipelines/graph/SqlGraphRegistrationContext.scala`

**Changes Needed:**
1. Import `InsertIntoDir` from catalyst
2. Add case handler for `InsertIntoDir` in pattern matching
3. Create `InsertIntoDirHandler` object
4. Register directory and flow in graph

---

### ✅ Step 4: Directory Write Execution (COMPLETED)
**Changes Made:**
1. ✅ Added `DirectoryWrite` class extending `FlowExecution`
2. ✅ Implemented `executeInternal()` method to write DataFrame to directory path
3. ✅ Updated `FlowPlanner.plan()` to handle Directory destinations
4. ✅ Supports format, mode, options, and partitioning

**Testing:** Will test with actual INSERT OVERWRITE DIRECTORY statements

**Files to Modify:**
- `sql/pipelines/src/main/scala/org/apache/spark/sql/pipelines/graph/FlowExecution.scala`

**Changes Needed:**
1. Add `DirectoryWrite` class extending `BatchFlowExecution`
2. Implement `executeInternal()` method
3. Update flow execution factory to handle Directory destinations

---

### ⏳ Step 5: Remove INSERT OVERWRITE Validation Block (TODO)
**Files to Modify:**
- `sql/pipelines/src/main/scala/org/apache/spark/sql/pipelines/graph/SqlGraphRegistrationContext.scala`

**Changes Needed:**
1. Update `validateInsertIntoFlow` to allow overwrite mode
2. Add warning log instead of throwing exception

---

### ⏳ Step 6: Python API Support (TODO)
**Files to Modify:**
- `python/pyspark/pipelines/api.py`

**Changes Needed:**
1. Add `@dp.directory_output` decorator
2. Implement `_register_directory_output` function
3. Handle protobuf message construction

---

### ⏳ Step 7: Unit Tests (TODO)
**Files to Create:**
- `sql/pipelines/src/test/scala/org/apache/spark/sql/pipelines/graph/DirectoryWriteSuite.scala`

**Test Cases:**
1. Basic INSERT OVERWRITE DIRECTORY with Parquet
2. Directory write with partitioning
3. Multiple runs (overwrite behavior)
4. Different file formats (CSV, JSON, ORC)
5. S3 path handling
6. Error cases (invalid path, unsupported format)

---

### ⏳ Step 8: Integration Tests (TODO)
**Files to Modify:**
- `sql/pipelines/src/test/scala/org/apache/spark/sql/pipelines/graph/SqlPipelineSuite.scala`

**Test Cases:**
1. E2E: Catalog table → Directory export
2. Mixed pipeline (tables + directories)
3. Dependency tracking with directories

---

### ⏳ Step 9: Documentation (TODO)
**Files to Update:**
- Add examples to project documentation
- Update programming guide

---

## Build Commands

### Build Protobuf
```bash
cd /Users/a.rana/Projects/spark-directory-write-feature
mvn clean install -DskipTests -pl :spark-connect-common_2.13
```

### Build Pipelines Module
```bash
mvn clean install -DskipTests -pl :spark-pipelines_2.13
```

### Run Tests
```bash
mvn test -pl :spark-pipelines_2.13 -Dtest=DirectoryWriteSuite
```

### Full Build
```bash
mvn clean package -DskipTests
```

---

## Next Steps

1. ✅ **Completed**: Protobuf modifications
2. **Next**: Build protobuf to generate Java classes
3. **Then**: Implement Directory case class in GraphElements.scala
4. **Then**: Add SQL handler for InsertIntoDir
5. **Then**: Implement DirectoryWrite execution logic

---

## Notes

- Leveraging existing `InsertIntoDataSourceDirCommand` from Spark SQL
- Directory outputs are non-catalog managed (no metadata tracking in metastore)
- Each directory write generates a unique identifier based on path hash for dependency tracking
- Mode defaults to "overwrite" for safety

---

## Testing Strategy

1. **Unit tests**: Test each component in isolation
2. **Integration tests**: Test end-to-end pipeline execution
3. **Manual testing**: Test with actual S3/HDFS paths
4. **Performance testing**: Test with large datasets
