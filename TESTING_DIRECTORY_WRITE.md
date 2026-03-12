# Testing INSERT OVERWRITE DIRECTORY Support

## Feature Summary

INSERT OVERWRITE DIRECTORY support has been implemented for Spark Declarative Pipelines.
This allows pipelines to export data to non-catalog managed directory paths.

## Implementation Status

### ✅ Completed Steps (1-4):

1. **Protobuf changes** - Added DIRECTORY output type and DirectoryDetails message
2. **Directory graph element** - Added Directory case class and graph integration
3. **SQL handler** - Added InsertIntoDirHandler to parse INSERT OVERWRITE DIRECTORY statements
4. **Execution logic** - Added DirectoryWrite class and FlowPlanner integration

### ⏳ Remaining Steps (5-9):

5. **Validation block removal** - Optional (doesn't affect directory writes)
6. **Python API support** - Not yet implemented
7. **Unit tests** - Not yet implemented
8. **Integration tests** - Not yet implemented
9. **Documentation** - This file is a start

## Test Example 1: Basic Parquet Export

```sql
-- Create source table
CREATE STREAMING TABLE events (
  event_id STRING,
  event_time TIMESTAMP,
  user_id STRING,
  action STRING
);

-- Ingest data
CREATE FLOW ingest_events AS INSERT INTO events BY NAME
SELECT * FROM STREAM source;

-- Export to directory
INSERT OVERWRITE DIRECTORY '/tmp/exports/events'
USING parquet
SELECT * FROM events
WHERE event_time >= CURRENT_DATE - INTERVAL 7 DAYS;
```

**Expected behavior:**
- Creates Directory output with identifier based on path hash
- Registers a CompleteFlow (batch) to write to the directory
- DirectoryWrite execution writes DataFrame to `/tmp/exports/events` in parquet format
- Mode is "overwrite" by default

## Test Example 2: Partitioned CSV Export

```sql
-- Export with partitioning
INSERT OVERWRITE DIRECTORY 's3://my-bucket/exports/users'
USING csv
OPTIONS (
  'header' = 'true',
  'compression' = 'gzip',
  'partitionBy' = 'country,signup_date'
)
SELECT
  user_id,
  email,
  country,
  DATE(signup_timestamp) as signup_date
FROM users;
```

**Expected behavior:**
- Writes to S3 path in CSV format
- Partitions by country and signup_date
- Applies gzip compression
- Includes header row in CSV files

## Test Example 3: ORC with Custom Options

```sql
-- Export to HDFS with ORC format
INSERT OVERWRITE DIRECTORY 'hdfs://namenode:9000/exports/transactions'
USING orc
OPTIONS (
  'compression' = 'snappy',
  'maxRecordsPerFile' = '1000000'
)
SELECT
  transaction_id,
  amount,
  currency,
  merchant_id,
  transaction_time
FROM transactions
WHERE transaction_time >= CURRENT_TIMESTAMP - INTERVAL 30 DAYS;
```

## Test Example 4: JSON Export

```sql
-- Export to local filesystem as JSON
INSERT OVERWRITE DIRECTORY 'file:///data/exports/alerts'
USING json
OPTIONS (
  'compression' = 'gzip'
)
SELECT * FROM alerts
WHERE severity IN ('HIGH', 'CRITICAL')
  AND alert_time >= CURRENT_TIMESTAMP - INTERVAL 1 DAY;
```

## Manual Testing Steps

### 1. Build the Project

```bash
cd /Users/a.rana/Projects/spark-directory-write-feature

# Build protobuf
mvn clean install -DskipTests -pl :spark-connect-common_2.13

# Build pipelines module
mvn clean install -DskipTests -pl :spark-pipelines_2.13

# Or full build
mvn clean package -DskipTests
```

### 2. Start Spark with Pipelines Support

```bash
# Start Spark shell with pipelines
./bin/spark-shell \
  --jars sql/pipelines/target/spark-pipelines_2.13-4.2.0-SNAPSHOT.jar

# Or start Spark SQL
./bin/spark-sql \
  --jars sql/pipelines/target/spark-pipelines_2.13-4.2.0-SNAPSHOT.jar
```

### 3. Run Test Queries

```sql
-- Create test pipeline
CREATE PIPELINE test_directory_export;

-- Create source table
CREATE STREAMING TABLE test_events (
  id INT,
  name STRING,
  timestamp TIMESTAMP
);

-- Test directory export
INSERT OVERWRITE DIRECTORY '/tmp/test-export'
USING parquet
SELECT * FROM test_events;

-- Verify export
SELECT * FROM parquet.`/tmp/test-export`;
```

### 4. Verify Directory Output

```bash
# Check that files were written
ls -lh /tmp/test-export/

# Expected output:
# part-00000-*.parquet
# _SUCCESS
```

### 5. Test with S3 (if available)

```sql
-- Configure S3 credentials
SET spark.hadoop.fs.s3a.access.key = 'your-access-key';
SET spark.hadoop.fs.s3a.secret.key = 'your-secret-key';

-- Export to S3
INSERT OVERWRITE DIRECTORY 's3a://your-bucket/exports/test'
USING parquet
OPTIONS (
  'compression' = 'snappy'
)
SELECT * FROM test_events;
```

## Expected Functionality

### Supported Features:

- ✅ **Path schemes**: s3://, s3a://, hdfs://, file:///, or relative paths
- ✅ **Formats**: parquet, orc, csv, json, avro, text
- ✅ **Write modes**: overwrite (default), append, errorifexists, ignore
- ✅ **Partitioning**: via partitionBy option
- ✅ **Compression**: snappy, gzip, lz4, etc.
- ✅ **Custom options**: maxRecordsPerFile, header, delimiter, etc.

### Not Supported (by design):

- ❌ **Streaming writes to directories**: Only batch (CompleteFlow) supported
- ❌ **Catalog registration**: Directories are not registered in the metastore
- ❌ **Schema enforcement**: No schema validation like tables
- ❌ **ACIDproperties**: No ACID guarantees

## Debugging

### Common Issues:

1. **Path not found error**
   - Ensure directory path is valid and accessible
   - Check filesystem permissions
   - Verify S3/HDFS credentials if using cloud storage

2. **Unsupported format error**
   - Verify format is one of: parquet, orc, csv, json, avro, text
   - Check that required format libraries are on classpath

3. **Compilation errors**
   - Ensure protobuf was rebuilt after proto changes
   - Check that all imports are correct in modified files
   - Verify Directory class is imported in FlowPlanner

### Log Verification:

```bash
# Check logs for directory registration
grep "Directory" spark-logs/spark-pipeline-*.log

# Check for flow execution
grep "DirectoryWrite" spark-logs/spark-pipeline-*.log
```

## Next Steps

1. **Complete Maven build** - Verify all code compiles
2. **Write unit tests** - DirectoryWriteSuite.scala
3. **Write integration tests** - Full pipeline with directory exports
4. **Add Python API** - @dp.directory_output decorator
5. **Performance testing** - Test with large datasets
6. **Documentation** - Update programming guide

## Implementation Files Modified

1. `sql/connect/common/src/main/protobuf/spark/connect/pipelines.proto`
   - Added DIRECTORY = 5 to OutputType enum
   - Added DirectoryDetails message

2. `sql/pipelines/src/main/scala/org/apache/spark/sql/pipelines/graph/elements.scala`
   - Added Directory case class

3. `sql/pipelines/src/main/scala/org/apache/spark/sql/pipelines/graph/DataflowGraph.scala`
   - Added directories: Seq[Directory]
   - Updated output map to include directories
   - Added directory lazy val

4. `sql/pipelines/src/main/scala/org/apache/spark/sql/pipelines/graph/GraphRegistrationContext.scala`
   - Added registerDirectory method
   - Added getDirectories method
   - Updated DataflowGraph construction

5. `sql/pipelines/src/main/scala/org/apache/spark/sql/pipelines/graph/SqlGraphRegistrationContext.scala`
   - Added InsertIntoDir import
   - Added case handler for InsertIntoDir
   - Implemented InsertIntoDirHandler object

6. `sql/pipelines/src/main/scala/org/apache/spark/sql/pipelines/graph/FlowExecution.scala`
   - Added DirectoryWrite class extending FlowExecution

7. `sql/pipelines/src/main/scala/org/apache/spark/sql/pipelines/graph/FlowPlanner.scala`
   - Updated plan() method to handle Directory destinations
   - Added Directory case to CompleteFlow matching

## Git Commits

```bash
# View commits
git log --oneline feature/directory-write-support

# Expected:
# d0eff7a [SPARK-XXXXX][SQL][PIPELINES] Step 4: Execution Logic
# 7bf59ed [SPARK-XXXXX][SQL][PIPELINES] Step 3: SQL Handler
# 45e96c1 [SPARK-XXXXX][SQL][PIPELINES] Step 2: Directory Graph Element
# e2c58df [SPARK-XXXXX][SQL][PIPELINES] Step 1: Protobuf Changes
```
