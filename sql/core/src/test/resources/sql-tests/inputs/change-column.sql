-- Create the origin table
CREATE TABLE test_change(a Int, b String, c Int);
DESC test_change;

-- Change column name (not supported yet)
ALTER TABLE test_change CHANGE a a1 Int;
ALTER TABLE test_change CHANGE b b1 String, c c1 Int;
DESC test_change;

-- Change column dataType (not supported yet)
ALTER TABLE test_change CHANGE a a String;
ALTER TABLE test_change CHANGE b b Int, c c Double;
DESC test_change;

-- Change column position (not supported yet)
ALTER TABLE test_change CHANGE a a Int AFTER b;
ALTER TABLE test_change CHANGE b b String FIRST, c c Int AFTER b;
DESC test_change;

-- Change column comment
ALTER TABLE test_change CHANGE a a Int COMMENT 'this is column a';
ALTER TABLE test_change CHANGE b b String COMMENT '#*02?`', c c Int COMMENT '';
DESC test_change;

-- Don't change anything.
ALTER TABLE test_change CHANGE a a Int COMMENT 'this is column a';
DESC test_change;

-- Change a invalid column
ALTER TABLE test_change CHANGE invalid_col invalid_col Int;
DESC test_change;

-- Change column name/dataType/position/comment together (not supported yet)
ALTER TABLE test_change CHANGE a a1 String COMMENT 'this is column a1' AFTER b;
DESC test_change;

-- Case sensitive
ALTER TABLE test_change CHANGE a A Int COMMENT 'this is column A';
DESC test_change;

-- Change column can't apply to a temporary/global_temporary view
CREATE TEMPORARY VIEW temp_view(a, b) AS SELECT 1, "one";
ALTER TABLE temp_view CHANGE a a Int COMMENT 'this is column a';
CREATE GLOBAL TEMPORARY VIEW global_temp_view(a, b) AS SELECT 1, "one";
ALTER TABLE global_temp.global_temp_view CHANGE a a Int COMMENT 'this is column a';

-- Change column in partition spec (not supported yet)
CREATE TABLE partition_table(a Int, b String) PARTITIONED BY (c Int, d String);
ALTER TABLE partition_table PARTITION (c = 1) CHANGE COLUMN a new_a Int;

-- DROP TEST TABLE
DROP TABLE test_change;
DROP TABLE partition_table;
