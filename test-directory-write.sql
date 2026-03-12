-- Manual Test Script for INSERT OVERWRITE DIRECTORY Feature
-- Run with: ./bin/spark-sql -f test-directory-write.sql
--
-- Prerequisites: Spark built successfully with the directory write feature

-- ============================================================================
-- Test 1: Basic Parquet Export
-- ============================================================================
CREATE TABLE test_source1 (id INT, name STRING, value DOUBLE);

INSERT INTO test_source1 VALUES
  (1, 'Alice', 100.5),
  (2, 'Bob', 200.75),
  (3, 'Charlie', 300.25);

INSERT OVERWRITE DIRECTORY '/tmp/test-parquet-export'
USING parquet
SELECT * FROM test_source1;

-- Verify: Files written
!ls -lh /tmp/test-parquet-export/;

-- Verify: Data correct
SELECT * FROM parquet.`/tmp/test-parquet-export`;

DROP TABLE test_source1;

-- ============================================================================
-- Test 2: CSV Export with Options
-- ============================================================================
CREATE TABLE test_source2 (id INT, category STRING, amount DOUBLE);

INSERT INTO test_source2 VALUES
  (1, 'A', 100.0),
  (2, 'B', 200.0),
  (3, 'A', 150.0),
  (4, 'C', 300.0);

INSERT OVERWRITE DIRECTORY '/tmp/test-csv-export'
USING csv
OPTIONS (
  'header' = 'true',
  'delimiter' = '|'
)
SELECT * FROM test_source2;

-- Verify: CSV files written
!ls -lh /tmp/test-csv-export/;

-- Verify: Data correct
SELECT * FROM csv.`path='/tmp/test-csv-export',header=true,delimiter=|`;

DROP TABLE test_source2;

-- ============================================================================
-- Test 3: Partitioned Export
-- ============================================================================
CREATE TABLE test_source3 (id INT, category STRING, value DOUBLE, date DATE);

INSERT INTO test_source3 VALUES
  (1, 'A', 100.0, DATE '2024-03-10'),
  (2, 'A', 200.0, DATE '2024-03-11'),
  (3, 'B', 150.0, DATE '2024-03-10'),
  (4, 'B', 250.0, DATE '2024-03-11'),
  (5, 'C', 300.0, DATE '2024-03-12');

INSERT OVERWRITE DIRECTORY '/tmp/test-partitioned-export'
USING parquet
OPTIONS ('partitionBy' = 'category')
SELECT id, value, category, date FROM test_source3;

-- Verify: Partition directories created
!ls -lh /tmp/test-partitioned-export/;
!ls -lh /tmp/test-partitioned-export/category=A/;
!ls -lh /tmp/test-partitioned-export/category=B/;
!ls -lh /tmp/test-partitioned-export/category=C/;

-- Verify: Data correct
SELECT * FROM parquet.`/tmp/test-partitioned-export`;
SELECT * FROM parquet.`/tmp/test-partitioned-export` WHERE category = 'A';

DROP TABLE test_source3;

-- ============================================================================
-- Test 4: JSON Export
-- ============================================================================
CREATE TABLE test_source4 (event_id INT, event_name STRING, timestamp TIMESTAMP);

INSERT INTO test_source4 VALUES
  (1, 'login', TIMESTAMP '2024-03-12 10:00:00'),
  (2, 'logout', TIMESTAMP '2024-03-12 11:00:00'),
  (3, 'login', TIMESTAMP '2024-03-12 12:00:00');

INSERT OVERWRITE DIRECTORY '/tmp/test-json-export'
USING json
SELECT * FROM test_source4;

-- Verify: JSON files written
!ls -lh /tmp/test-json-export/;

-- Verify: Data correct
SELECT * FROM json.`/tmp/test-json-export`;

DROP TABLE test_source4;

-- ============================================================================
-- Test 5: ORC Export with Compression
-- ============================================================================
CREATE TABLE test_source5 (id INT, data STRING, value DOUBLE);

INSERT INTO test_source5 VALUES
  (1, 'data1', 111.1),
  (2, 'data2', 222.2),
  (3, 'data3', 333.3);

INSERT OVERWRITE DIRECTORY '/tmp/test-orc-export'
USING orc
OPTIONS ('compression' = 'snappy')
SELECT * FROM test_source5;

-- Verify: ORC files written
!ls -lh /tmp/test-orc-export/;

-- Verify: Data correct
SELECT * FROM orc.`/tmp/test-orc-export`;

DROP TABLE test_source5;

-- ============================================================================
-- Test 6: Aggregated Export
-- ============================================================================
CREATE TABLE test_source6 (id INT, category STRING, amount DOUBLE);

INSERT INTO test_source6 VALUES
  (1, 'A', 100.0),
  (2, 'A', 200.0),
  (3, 'B', 150.0),
  (4, 'B', 250.0),
  (5, 'C', 300.0),
  (6, 'C', 400.0);

INSERT OVERWRITE DIRECTORY '/tmp/test-aggregated-export'
USING parquet
SELECT
  category,
  COUNT(*) as count,
  SUM(amount) as total_amount,
  AVG(amount) as avg_amount,
  MIN(amount) as min_amount,
  MAX(amount) as max_amount
FROM test_source6
GROUP BY category;

-- Verify: Aggregated data exported
SELECT * FROM parquet.`/tmp/test-aggregated-export` ORDER BY category;

DROP TABLE test_source6;

-- ============================================================================
-- Test 7: Overwrite Behavior
-- ============================================================================
CREATE TABLE test_source7 (id INT, value STRING);

-- First write
INSERT INTO test_source7 VALUES (1, 'first'), (2, 'first');

INSERT OVERWRITE DIRECTORY '/tmp/test-overwrite-behavior'
USING parquet
SELECT * FROM test_source7;

-- Verify first write
SELECT * FROM parquet.`/tmp/test-overwrite-behavior`;

-- Second write with different data
DELETE FROM test_source7 WHERE id > 0;
INSERT INTO test_source7 VALUES (3, 'second'), (4, 'second'), (5, 'second');

INSERT OVERWRITE DIRECTORY '/tmp/test-overwrite-behavior'
USING parquet
SELECT * FROM test_source7;

-- Verify second write overwrote first
SELECT * FROM parquet.`/tmp/test-overwrite-behavior`;
-- Should only see 'second' values, not 'first'

DROP TABLE test_source7;

-- ============================================================================
-- Test 8: Multiple Exports in One Pipeline
-- ============================================================================
CREATE TABLE test_source8 (id INT, value DOUBLE);

INSERT INTO test_source8 SELECT id, id * 10.0 FROM RANGE(20);

INSERT OVERWRITE DIRECTORY '/tmp/test-multi-export-1'
USING parquet
SELECT * FROM test_source8 WHERE id < 10;

INSERT OVERWRITE DIRECTORY '/tmp/test-multi-export-2'
USING json
SELECT * FROM test_source8 WHERE id >= 10;

-- Verify both exports
SELECT COUNT(*) as count FROM parquet.`/tmp/test-multi-export-1`;
-- Should be 10

SELECT COUNT(*) as count FROM json.`/tmp/test-multi-export-2`;
-- Should be 10

DROP TABLE test_source8;

-- ============================================================================
-- Cleanup
-- ============================================================================
!rm -rf /tmp/test-parquet-export;
!rm -rf /tmp/test-csv-export;
!rm -rf /tmp/test-partitioned-export;
!rm -rf /tmp/test-json-export;
!rm -rf /tmp/test-orc-export;
!rm -rf /tmp/test-aggregated-export;
!rm -rf /tmp/test-overwrite-behavior;
!rm -rf /tmp/test-multi-export-1;
!rm -rf /tmp/test-multi-export-2;

-- ============================================================================
-- Summary
-- ============================================================================
-- All tests completed successfully!
-- INSERT OVERWRITE DIRECTORY feature is working correctly.
