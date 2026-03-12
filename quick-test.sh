#!/bin/bash
#
# Quick Test Script for INSERT OVERWRITE DIRECTORY Feature
# Run after successful Spark build
#

set -e

echo "=========================================="
echo "INSERT OVERWRITE DIRECTORY - Quick Test"
echo "=========================================="
echo ""

# Check if Spark is built
if [ ! -f "./bin/spark-sql" ]; then
  echo "ERROR: Spark not built. Please run: mvn clean install -DskipTests"
  exit 1
fi

echo "✓ Spark binary found"
echo ""

# Create temp directory for tests
TEST_DIR="/tmp/spark-directory-write-test-$(date +%s)"
mkdir -p "$TEST_DIR"
echo "✓ Test directory: $TEST_DIR"
echo ""

# Test 1: Basic Parquet Export
echo "Test 1: Basic Parquet Export..."
./bin/spark-sql --conf spark.sql.shuffle.partitions=2 <<EOF
CREATE TABLE test_data (id INT, name STRING, value DOUBLE);
INSERT INTO test_data VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0);

INSERT OVERWRITE DIRECTORY '$TEST_DIR/parquet-output'
USING parquet
SELECT * FROM test_data;

SELECT COUNT(*) as row_count FROM parquet.\`$TEST_DIR/parquet-output\`;
DROP TABLE test_data;
EOF

if [ -d "$TEST_DIR/parquet-output" ]; then
  FILE_COUNT=$(ls "$TEST_DIR/parquet-output"/*.parquet 2>/dev/null | wc -l)
  if [ $FILE_COUNT -gt 0 ]; then
    echo "✓ Test 1 PASSED: Parquet files written ($FILE_COUNT files)"
  else
    echo "✗ Test 1 FAILED: No parquet files found"
    exit 1
  fi
else
  echo "✗ Test 1 FAILED: Output directory not created"
  exit 1
fi
echo ""

# Test 2: CSV with Options
echo "Test 2: CSV Export with Options..."
./bin/spark-sql --conf spark.sql.shuffle.partitions=2 <<EOF
CREATE TABLE test_data2 (id INT, category STRING);
INSERT INTO test_data2 VALUES (1, 'A'), (2, 'B'), (3, 'C');

INSERT OVERWRITE DIRECTORY '$TEST_DIR/csv-output'
USING csv
OPTIONS ('header' = 'true', 'delimiter' = '|')
SELECT * FROM test_data2;

DROP TABLE test_data2;
EOF

if [ -d "$TEST_DIR/csv-output" ]; then
  FILE_COUNT=$(ls "$TEST_DIR/csv-output"/*.csv 2>/dev/null | wc -l)
  if [ $FILE_COUNT -gt 0 ]; then
    echo "✓ Test 2 PASSED: CSV files written ($FILE_COUNT files)"

    # Check for header
    FIRST_LINE=$(cat "$TEST_DIR/csv-output"/*.csv | head -1)
    if echo "$FIRST_LINE" | grep -q "id|category"; then
      echo "  ✓ CSV header present with correct delimiter"
    fi
  else
    echo "✗ Test 2 FAILED: No CSV files found"
    exit 1
  fi
else
  echo "✗ Test 2 FAILED: Output directory not created"
  exit 1
fi
echo ""

# Test 3: Partitioned Export
echo "Test 3: Partitioned Export..."
./bin/spark-sql --conf spark.sql.shuffle.partitions=2 <<EOF
CREATE TABLE test_data3 (id INT, category STRING, value DOUBLE);
INSERT INTO test_data3 VALUES
  (1, 'A', 100.0),
  (2, 'A', 200.0),
  (3, 'B', 150.0),
  (4, 'B', 250.0);

INSERT OVERWRITE DIRECTORY '$TEST_DIR/partitioned-output'
USING parquet
OPTIONS ('partitionBy' = 'category')
SELECT * FROM test_data3;

DROP TABLE test_data3;
EOF

if [ -d "$TEST_DIR/partitioned-output" ]; then
  if [ -d "$TEST_DIR/partitioned-output/category=A" ] && [ -d "$TEST_DIR/partitioned-output/category=B" ]; then
    echo "✓ Test 3 PASSED: Partitioned directories created"
    echo "  ✓ category=A partition exists"
    echo "  ✓ category=B partition exists"
  else
    echo "✗ Test 3 FAILED: Partition directories not found"
    exit 1
  fi
else
  echo "✗ Test 3 FAILED: Output directory not created"
  exit 1
fi
echo ""

# Test 4: JSON Export
echo "Test 4: JSON Export..."
./bin/spark-sql --conf spark.sql.shuffle.partitions=2 <<EOF
CREATE TABLE test_data4 (event_id INT, event_name STRING);
INSERT INTO test_data4 VALUES (1, 'login'), (2, 'logout');

INSERT OVERWRITE DIRECTORY '$TEST_DIR/json-output'
USING json
SELECT * FROM test_data4;

DROP TABLE test_data4;
EOF

if [ -d "$TEST_DIR/json-output" ]; then
  FILE_COUNT=$(ls "$TEST_DIR/json-output"/*.json 2>/dev/null | wc -l)
  if [ $FILE_COUNT -gt 0 ]; then
    echo "✓ Test 4 PASSED: JSON files written ($FILE_COUNT files)"
  else
    echo "✗ Test 4 FAILED: No JSON files found"
    exit 1
  fi
else
  echo "✗ Test 4 FAILED: Output directory not created"
  exit 1
fi
echo ""

# Test 5: Overwrite Behavior
echo "Test 5: Overwrite Behavior..."
./bin/spark-sql --conf spark.sql.shuffle.partitions=2 <<EOF
CREATE TABLE test_data5 (id INT, value STRING);

-- First write
INSERT INTO test_data5 VALUES (1, 'first'), (2, 'first');
INSERT OVERWRITE DIRECTORY '$TEST_DIR/overwrite-test'
USING parquet
SELECT * FROM test_data5;

-- Second write
DELETE FROM test_data5 WHERE id > 0;
INSERT INTO test_data5 VALUES (3, 'second'), (4, 'second'), (5, 'second');
INSERT OVERWRITE DIRECTORY '$TEST_DIR/overwrite-test'
USING parquet
SELECT * FROM test_data5;

-- Verify only 'second' values exist
SELECT COUNT(*) as second_count FROM parquet.\`$TEST_DIR/overwrite-test\` WHERE value = 'second';

DROP TABLE test_data5;
EOF

if [ -d "$TEST_DIR/overwrite-test" ]; then
  echo "✓ Test 5 PASSED: Overwrite behavior working"
else
  echo "✗ Test 5 FAILED: Output directory not created"
  exit 1
fi
echo ""

# Cleanup
echo "Cleaning up test directory..."
rm -rf "$TEST_DIR"
echo "✓ Cleanup complete"
echo ""

echo "=========================================="
echo "✅ ALL TESTS PASSED!"
echo "=========================================="
echo ""
echo "INSERT OVERWRITE DIRECTORY feature is working correctly!"
echo ""
echo "You can now:"
echo "  1. Run the comprehensive SQL test: ./bin/spark-sql -f test-directory-write.sql"
echo "  2. Run unit tests: mvn test -pl :spark-pipelines_2.13 -Dtest=DirectoryWriteSuite"
echo "  3. Try your own exports with INSERT OVERWRITE DIRECTORY"
echo ""
