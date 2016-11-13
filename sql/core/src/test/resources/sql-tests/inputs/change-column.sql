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

-- Change column name/dataType/position/comment together (not supported yet)
ALTER TABLE test_change CHANGE a a1 String COMMENT 'this is column a1' AFTER b;
DESC test_change;

-- Change column can't apply to a temporary/global_temporary view
CREATE TEMPORARY VIEW temp_view(a, b) AS SELECT 1, "one";
ALTER TABLE temp_view CHANGE a a Int COMMENT 'this is column a';
CREATE GLOBAL TEMPORARY VIEW global_temp_view(a, b) AS SELECT 1, "one";
ALTER TABLE global_temp.global_temp_view CHANGE a a Int COMMENT 'this is column a';

-- DROP TEST TABLE
DROP TABLE test_change;
