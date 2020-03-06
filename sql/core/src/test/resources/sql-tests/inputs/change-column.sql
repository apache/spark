-- Create the origin table
CREATE TABLE test_change(a INT, b STRING, c INT) using parquet;
DESC test_change;

-- ALTER TABLE CHANGE COLUMN must change either type or comment
ALTER TABLE test_change CHANGE a;
DESC test_change;

-- Change column name (not supported on v1 table)
ALTER TABLE test_change RENAME COLUMN a TO a1;
DESC test_change;

-- Change column dataType (not supported yet)
ALTER TABLE test_change CHANGE a TYPE STRING;
DESC test_change;

-- Change column position (not supported yet)
ALTER TABLE test_change CHANGE a AFTER b;
ALTER TABLE test_change CHANGE b FIRST;
DESC test_change;

-- Change column comment
ALTER TABLE test_change CHANGE a COMMENT 'this is column a';
ALTER TABLE test_change CHANGE b COMMENT '#*02?`';
ALTER TABLE test_change CHANGE c COMMENT '';
DESC test_change;

-- Don't change anything.
ALTER TABLE test_change CHANGE a TYPE INT;
ALTER TABLE test_change CHANGE a COMMENT 'this is column a';
DESC test_change;

-- Change a invalid column
ALTER TABLE test_change CHANGE invalid_col TYPE INT;
DESC test_change;

-- Check case insensitivity.
ALTER TABLE test_change CHANGE A COMMENT 'case insensitivity';
DESC test_change;

-- Change column can't apply to a temporary/global_temporary view
CREATE TEMPORARY VIEW temp_view(a, b) AS SELECT 1, "one";
ALTER TABLE temp_view CHANGE a TYPE INT;
CREATE GLOBAL TEMPORARY VIEW global_temp_view(a, b) AS SELECT 1, "one";
ALTER TABLE global_temp.global_temp_view CHANGE a TYPE INT;

-- DROP TEST TABLE
DROP TABLE test_change;
DROP VIEW global_temp.global_temp_view;
