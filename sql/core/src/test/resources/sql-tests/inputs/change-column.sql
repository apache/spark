-- Create the origin table
CREATE TABLE test_change(a Int, b String, c Int);
CREATE VIEW test_view(a, b, c) AS
SELECT * FROM VALUES (1, "one", 11), (null, "two", 22) AS testData(a, b, c);
DESC test_change;
DESC test_view;

-- Change column name (not supported yet)
ALTER TABLE test_change CHANGE a a1 Int;
ALTER TABLE test_change CHANGE b b1 String, c c1 Int;
DESC test_change;
ALTER VIEW test_view CHANGE a a1 Int;
DESC test_view;

-- Change column dataType (not supported yet)
ALTER TABLE test_change CHANGE a a String;
ALTER TABLE test_change CHANGE b b Int, c c Double;
DESC test_change;
ALTER VIEW test_view CHANGE a a String;
DESC test_view;

-- Change column position (not supported yet)
ALTER TABLE test_change CHANGE a a Int AFTER b;
ALTER TABLE test_change CHANGE b b String FIRST, c c Int AFTER b;
DESC test_change;
ALTER VIEW test_view CHANGE c c Int FIRST;
DESC test_view;

-- Change column comment
ALTER TABLE test_change CHANGE a a Int COMMENT 'this is column a';
ALTER TABLE test_change CHANGE b b String COMMENT '#*02?`', c c Int COMMENT '';
DESC test_change;
ALTER VIEW test_view CHANGE a a Int COMMENT 'this is column a';
DESC test_view;

-- Change column name/dataType/position/comment together (not supported yet)
ALTER TABLE test_change CHANGE a a1 String COMMENT 'this is column a1' AFTER b;
DESC test_change;
ALTER VIEW test_view CHANGE a a1 String COMMENT 'this is column a1' AFTER b;
DESC test_view;

-- Change column can't apply to a temporary/global_temporary view
CREATE TEMPORARY VIEW temp_view(a, b) AS SELECT 1, "one";
ALTER VIEW temp_view CHANGE a a Int COMMENT 'this is column a';
CREATE GLOBAL TEMPORARY VIEW global_temp_view(a, b) AS SELECT 1, "one";
ALTER VIEW global_temp.global_temp_view CHANGE a a Int COMMENT 'this is column a';

-- DROP TEST TABLE
DROP TABLE test_change;
DROP VIEW test_view;
