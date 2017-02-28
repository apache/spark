-- Tests for qualified column names for the view code-path
-- Test scenario with Temporary view
CREATE OR REPLACE TEMPORARY VIEW table1 AS SELECT 2 AS i1;
SELECT table1.* FROM table1;
SELECT * FROM table1;
SELECT table1.i1 FROM table1;
SELECT i1 FROM table1;
SELECT a.i1 FROM table1 AS a;
SELECT i1 FROM table1 AS a;
-- cleanup
DROP VIEW table1;

-- Test scenario with Global Temp view
CREATE OR REPLACE GLOBAL TEMPORARY VIEW t1 as SELECT 1 as i1;
SELECT * FROM global_temp.t1;
-- TODO: Support this scenario
SELECT global_temp.t1.* FROM global_temp.t1;
SELECT i1 FROM global_temp.t1;
-- TODO: Support this scenario
SELECT global_temp.t1.i1 FROM global_temp.t1;
SELECT t1.i1 FROM global_temp.t1;
SELECT a.i1 FROM global_temp.t1 AS a;
SELECT i1 FROM global_temp.t1 AS a;
-- cleanup
DROP VIEW global_temp.t1;
