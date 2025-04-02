-- Tests for qualified column names for the view code-path
-- Test scenario with Temporary view
CREATE OR REPLACE TEMPORARY VIEW view1 AS SELECT 2 AS i1;
SELECT view1.* FROM view1;
SELECT * FROM view1;
SELECT view1.i1 FROM view1;
SELECT i1 FROM view1;
SELECT a.i1 FROM view1 AS a;
SELECT i1 FROM view1 AS a;
-- cleanup
DROP VIEW view1;

-- Test scenario with Global Temp view
CREATE OR REPLACE GLOBAL TEMPORARY VIEW view1 as SELECT 1 as i1;
SELECT * FROM global_temp.view1;
SELECT global_temp.view1.* FROM global_temp.view1;
SELECT i1 FROM global_temp.view1;
SELECT global_temp.view1.i1 FROM global_temp.view1;
SELECT view1.i1 FROM global_temp.view1;
SELECT a.i1 FROM global_temp.view1 AS a;
SELECT i1 FROM global_temp.view1 AS a;
-- cleanup
DROP VIEW global_temp.view1;
