-- Create the origin table
CREATE TABLE test_change(a INT, b STRING, c INT) using parquet;
DESC test_change;

-- Change column name (not supported yet)
ALTER TABLE test_change CHANGE a a1 INT;
DESC test_change;

-- Change column dataType (not supported yet)
ALTER TABLE test_change CHANGE a a STRING;
DESC test_change;

-- Change column position (not supported yet)
ALTER TABLE test_change CHANGE a a INT AFTER b;
ALTER TABLE test_change CHANGE b b STRING FIRST;
DESC test_change;

-- Change column comment
ALTER TABLE test_change CHANGE a a INT COMMENT 'this is column a';
ALTER TABLE test_change CHANGE b b STRING COMMENT '#*02?`';
ALTER TABLE test_change CHANGE c c INT COMMENT '';
DESC test_change;

-- Don't change anything.
ALTER TABLE test_change CHANGE a a INT COMMENT 'this is column a';
DESC test_change;

-- Change a invalid column
ALTER TABLE test_change CHANGE invalid_col invalid_col INT;
DESC test_change;

-- Change column name/dataType/position/comment together (not supported yet)
ALTER TABLE test_change CHANGE a a1 STRING COMMENT 'this is column a1' AFTER b;
DESC test_change;

-- Check the behavior with different values of CASE_SENSITIVE
SET spark.sql.caseSensitive=false;
ALTER TABLE test_change CHANGE a A INT COMMENT 'this is column A';
SET spark.sql.caseSensitive=true;
ALTER TABLE test_change CHANGE a A INT COMMENT 'this is column A1';
DESC test_change;

-- Change column can't apply to a temporary/global_temporary view
CREATE TEMPORARY VIEW temp_view(a, b) AS SELECT 1, "one";
ALTER TABLE temp_view CHANGE a a INT COMMENT 'this is column a';
CREATE GLOBAL TEMPORARY VIEW global_temp_view(a, b) AS SELECT 1, "one";
ALTER TABLE global_temp.global_temp_view CHANGE a a INT COMMENT 'this is column a';

-- Change column in partition spec (not supported yet)
CREATE TABLE partition_table(a INT, b STRING, c INT, d STRING) USING parquet PARTITIONED BY (c, d);
ALTER TABLE partition_table PARTITION (c = 1) CHANGE COLUMN a new_a INT;
ALTER TABLE partition_table CHANGE COLUMN c c INT COMMENT 'this is column C';

-- DROP TEST TABLE
DROP TABLE test_change;
DROP TABLE partition_table;
