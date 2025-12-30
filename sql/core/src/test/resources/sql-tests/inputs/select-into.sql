-- Comprehensive test suite for SELECT INTO
-- Tests all error conditions and valid use cases
--
-- TABLE OF CONTENTS
-- =================
-- Feature Flag Tests:
--   1. Disable SELECT INTO feature flag
--   2. SELECT INTO rejected when feature disabled
--   3. Re-enable SELECT INTO feature flag
--   4. Verify SELECT INTO works after re-enabling
--
-- Error Tests:
--   5. SELECT INTO outside SQL script (not in BEGIN...END)
--   6. SELECT INTO in subquery
--   7. SELECT INTO with UNION
--   8. SELECT INTO with INTERSECT
--   9. SELECT INTO with EXCEPT
--  10. Too many variables (3 vars, 2 columns)
--  11. Too few variables (1 var, 2 columns)
--  12. Variable count mismatch with expressions
--  13. Struct field count mismatch
--  14. Multiple rows returned (error)
--
-- Valid Cases:
--  15. Single column into single variable
--  16. Multiple columns into multiple variables
--  17. Expressions into variables
--  18. Struct unpacking (multiple columns into single struct)
--  19. Struct field access after unpacking
--
-- Zero Rows Behavior:
--  20. Multiple variables remain unchanged on zero rows
--  21. Single variable remains unchanged on zero rows
--  22. NULL variables remain NULL on zero rows
--  23. Struct variables remain unchanged on zero rows
--
-- Edge Cases:
--  24. SELECT * with INTO
--  25. Column aliases with INTO
-- =================

-- Setup test data
CREATE TEMPORARY VIEW tbl_view AS SELECT * FROM VALUES
  (10, 'name1'),
  (20, 'name2'),
  (30, 'name3')
AS tbl_view(id, name);

-- =============================================================================
-- Test 1: Disable SELECT INTO feature flag
-- =============================================================================

-- !query
SET spark.sql.scripting.selectIntoEnabled=false;
-- !query schema
struct<key:string,value:string>
-- !query output
spark.sql.scripting.selectIntoEnabled	false

-- =============================================================================
-- Test 2: SELECT INTO rejected when feature disabled
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  SELECT id INTO v1 FROM tbl_view WHERE id = 10;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_FEATURE_DISABLED",
  "sqlState" : "0A000"
}

-- =============================================================================
-- Test 3: Re-enable SELECT INTO feature flag
-- =============================================================================

-- !query
SET spark.sql.scripting.selectIntoEnabled=true;
-- !query schema
struct<key:string,value:string>
-- !query output
spark.sql.scripting.selectIntoEnabled	true

-- =============================================================================
-- Test 4: Verify SELECT INTO works after re-enabling
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  SELECT id INTO v1 FROM tbl_view WHERE id = 10;
  SELECT v1;
END;
-- !query schema
struct<v1:int>
-- !query output
10

-- =============================================================================
-- Test 5: SELECT INTO outside SQL script
-- =============================================================================

-- !query
SELECT id INTO res_id FROM tbl_view WHERE id = 10;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_NOT_IN_SQL_SCRIPT",
  "sqlState" : "0A000"
}

-- =============================================================================
-- Test 6: SELECT INTO in subquery
-- =============================================================================

-- !query
BEGIN
  DECLARE outer_id INT;
  SELECT (SELECT id INTO outer_id FROM tbl_view WHERE id = 10) as nested;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_ONLY_AT_TOP_LEVEL",
  "sqlState" : "42601"
}

-- =============================================================================
-- Test 7: SELECT INTO with UNION
-- =============================================================================

-- !query
BEGIN
  DECLARE res_id INT;
  SELECT id INTO res_id FROM tbl_view WHERE id = 10
  UNION
  SELECT id FROM tbl_view WHERE id = 20;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_ONLY_AT_TOP_LEVEL",
  "sqlState" : "42601"
}

-- =============================================================================
-- Test 8: SELECT INTO with INTERSECT
-- =============================================================================

-- !query
BEGIN
  DECLARE res_id INT;
  SELECT id INTO res_id FROM tbl_view WHERE id = 10
  INTERSECT
  SELECT id FROM tbl_view WHERE id = 10;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_ONLY_AT_TOP_LEVEL",
  "sqlState" : "42601"
}

-- =============================================================================
-- Test 9: SELECT INTO with EXCEPT
-- =============================================================================

-- !query
BEGIN
  DECLARE res_id INT;
  SELECT id INTO res_id FROM tbl_view WHERE id = 10
  EXCEPT
  SELECT id FROM tbl_view WHERE id = 20;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_ONLY_AT_TOP_LEVEL",
  "sqlState" : "42601"
}

-- =============================================================================
-- Test 10: Too many variables (3 vars, 2 columns)
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  DECLARE v2 STRING;
  DECLARE v3 INT;
  SELECT id, name INTO v1, v2, v3 FROM tbl_view WHERE id = 10;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_VARIABLE_COUNT_MISMATCH",
  "sqlState" : "21000",
  "messageParameters" : {
    "numCols" : "2",
    "numVars" : "3"
  }
}

-- =============================================================================
-- Test 11: Too few variables (1 var, 2 columns)
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  SELECT id, name INTO v1 FROM tbl_view WHERE id = 10;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_VARIABLE_COUNT_MISMATCH",
  "sqlState" : "21000",
  "messageParameters" : {
    "numCols" : "2",
    "numVars" : "1"
  }
}

-- =============================================================================
-- Test 12: Variable count mismatch with expressions
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  DECLARE v2 INT;
  DECLARE v3 INT;
  SELECT id + 10, id * 2 INTO v1, v2, v3 FROM tbl_view WHERE id = 10;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_VARIABLE_COUNT_MISMATCH",
  "sqlState" : "21000",
  "messageParameters" : {
    "numCols" : "2",
    "numVars" : "3"
  }
}

-- =============================================================================
-- Test 13: Struct field count mismatch
-- =============================================================================

-- !query
BEGIN
  DECLARE result_struct STRUCT<f1: INT, f2: STRING>;
  SELECT id, name, id + 10 INTO result_struct FROM tbl_view WHERE id = 10;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_STRUCT_FIELD_MISMATCH",
  "sqlState" : "21000",
  "messageParameters" : {
    "numCols" : "3",
    "numFields" : "2",
    "structType" : "\"STRUCT<f1: INT, f2: STRING>\""
  }
}

-- =============================================================================
-- Test 14: Multiple rows returned
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  SELECT id INTO v1 FROM tbl_view;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.SparkException
{
  "errorClass" : "ROW_SUBQUERY_TOO_MANY_ROWS",
  "sqlState" : "21000"
}

-- =============================================================================
-- Test 15: Single column into single variable
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  SELECT id INTO v1 FROM tbl_view WHERE id = 30;
  SELECT v1;
END;
-- !query schema
struct<v1:int>
-- !query output
30

-- =============================================================================
-- Test 16: Multiple columns into multiple variables
-- =============================================================================

-- !query
BEGIN
  DECLARE var1 INT;
  DECLARE var2 STRING;
  SELECT id, name INTO var1, var2 FROM tbl_view WHERE id = 20;
  SELECT var1, var2;
END;
-- !query schema
struct<var1:int,var2:string>
-- !query output
20	name2

-- =============================================================================
-- Test 17: Expressions into variables
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  DECLARE v2 INT;
  SELECT id + 10, id * 2 INTO v1, v2 FROM tbl_view WHERE id = 10;
  SELECT v1, v2;
END;
-- !query schema
struct<v1:int,v2:int>
-- !query output
20	20

-- =============================================================================
-- Test 18: Struct unpacking (multiple columns into single struct)
-- =============================================================================

-- !query
BEGIN
  DECLARE result_struct STRUCT<field1: INT, field2: STRING>;
  SELECT id, name INTO result_struct FROM tbl_view WHERE id = 10;
  SELECT result_struct;
END;
-- !query schema
struct<result_struct:struct<field1:int,field2:string>>
-- !query output
{"field1":10,"field2":"name1"}

-- =============================================================================
-- Test 19: Struct field access after unpacking
-- =============================================================================

-- !query
BEGIN
  DECLARE my_struct STRUCT<a: INT, b: STRING>;
  SELECT id, name INTO my_struct FROM tbl_view WHERE id = 20;
  SELECT my_struct.a, my_struct.b;
END;
-- !query schema
struct<a:int,b:string>
-- !query output
20	name2

-- =============================================================================
-- Test 20: Multiple variables remain unchanged on zero rows
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  DECLARE v2 STRING;
  SET VAR v1 = 99;
  SET VAR v2 = 'initial';
  SELECT v1, v2;
  SELECT id, name INTO v1, v2 FROM tbl_view WHERE id = 999;
  SELECT v1, v2;
END;
-- !query schema
struct<v1:int,v2:string>
-- !query output
99	initial
99	initial

-- =============================================================================
-- Test 21: Single variable remains unchanged on zero rows
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  SET VAR v1 = 42;
  SELECT v1;
  SELECT id INTO v1 FROM tbl_view WHERE 1=0;
  SELECT v1;
END;
-- !query schema
struct<v1:int>
-- !query output
42
42

-- =============================================================================
-- Test 22: NULL variables remain NULL on zero rows
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  SELECT v1;
  SELECT id INTO v1 FROM tbl_view WHERE FALSE;
  SELECT v1;
END;
-- !query schema
struct<v1:int>
-- !query output
NULL
NULL

-- =============================================================================
-- Test 23: Struct variables remain unchanged on zero rows
-- =============================================================================

-- !query
BEGIN
  DECLARE my_struct STRUCT<x: INT, y: STRING>;
  SET VAR my_struct = named_struct('x', 100, 'y', 'original');
  SELECT my_struct;
  SELECT id, name INTO my_struct FROM tbl_view WHERE id < 0;
  SELECT my_struct;
END;
-- !query schema
struct<my_struct:struct<x:int,y:string>>
-- !query output
{"x":100,"y":"original"}
{"x":100,"y":"original"}

-- =============================================================================
-- Test 24: SELECT * with INTO
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  DECLARE v2 STRING;
  SELECT * INTO v1, v2 FROM tbl_view WHERE id = 10;
  SELECT v1, v2;
END;
-- !query schema
struct<v1:int,v2:string>
-- !query output
10	name1

-- =============================================================================
-- Test 25: Column aliases with INTO
-- =============================================================================

-- !query
BEGIN
  DECLARE a INT;
  DECLARE b STRING;
  SELECT id AS my_id, name AS my_name INTO a, b FROM tbl_view WHERE id = 30;
  SELECT a, b;
END;
-- !query schema
struct<a:int,b:string>
-- !query output
30	name3

-- Clean up
DROP VIEW tbl_view;
