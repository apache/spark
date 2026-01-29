-- Comprehensive test suite for SELECT INTO
-- Tests all error conditions and valid use cases
--
-- TABLE OF CONTENTS
-- =================
-- A. FEATURE CONFIGURATION
--   1. Enable ANSI mode for overflow detection
--   2. Disable SELECT INTO feature flag
--   3. SELECT INTO rejected when feature disabled
--   4. Re-enable SELECT INTO feature flag
--
-- B. BASIC USAGE
--   5. Single column into single variable
--   6. Multiple columns into multiple variables
--   7. Expressions into variables
--   8. SELECT * with INTO
--   9. Column aliases with INTO
--
-- C. TYPE CASTING
--  10. Implicit upcast (INT to BIGINT)
--  11. Implicit downcast with precision loss (DOUBLE to INT)
--  12. Invalid cast error (STRING to INT)
--  13. Numeric overflow (large DOUBLE to INT)
--  14. Numeric overflow (INT to SMALLINT)
--
-- D. VARIABLE SCOPING
--  15. Local and session variables combined
--  16. Qualified variable names (system.session.varname)
--  17. IDENTIFIER clause for variable names
--
-- E. STRUCT HANDLING
--  18. Struct unpacking (multiple columns into single struct)
--  19. Struct field access after unpacking
--  20. Struct field count mismatch (error)
--
-- F. ZERO ROWS BEHAVIOR (NO DATA CONDITION)
--  21. SELECT INTO raises NO DATA (SQLSTATE 02000) on zero rows - unhandled
--  22. SELECT INTO with CONTINUE HANDLER for NOT FOUND
--  23. SELECT INTO with CONTINUE HANDLER for SQLSTATE '02000'
--  24. SELECT INTO with EXIT HANDLER for NOT FOUND
--  24b. SELECT INTO NO DATA with struct variable and handler
--
-- G. RESULT SET BEHAVIOR
--  25. SELECT INTO does not return a result set
--
-- H. ERROR CASES - CONTEXT/PLACEMENT
--  26. SELECT INTO outside SQL script (not in BEGIN...END)
--  27. SELECT INTO in subquery
--  28. SELECT INTO in UNION (first branch)
--  29. SELECT INTO in UNION (last branch)
--  30. SELECT INTO in INTERSECT
--  31. SELECT INTO in EXCEPT
--
-- I. ERROR CASES - DATA ISSUES
--  32. Too many variables (3 vars, 2 columns)
--  33. Too few variables (1 var, 2 columns)
--  34. Variable count mismatch with expressions
--  35. Multiple rows returned (error)
-- =================

-- =============================================================================
-- SECTION A: FEATURE CONFIGURATION
-- =============================================================================

-- =============================================================================
-- Test 1: Enable ANSI mode for overflow detection
-- =============================================================================

-- !query
SET spark.sql.ansi.enabled = true;
-- !query schema
struct<key:string,value:string>
-- !query output
spark.sql.ansi.enabled	true

-- Setup test data
CREATE TEMPORARY VIEW tbl_view AS SELECT * FROM VALUES
  (10, 'name1'),
  (20, 'name2'),
  (30, 'name3')
AS tbl_view(id, name);

-- =============================================================================
-- Test 2: Disable SELECT INTO feature flag
-- =============================================================================

-- !query
SET spark.sql.scripting.selectIntoEnabled=false;
-- !query schema
struct<key:string,value:string>
-- !query output
spark.sql.scripting.selectIntoEnabled	false

-- =============================================================================
-- Test 3: SELECT INTO rejected when feature disabled
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
-- Test 4: Re-enable SELECT INTO feature flag
-- =============================================================================

-- !query
SET spark.sql.scripting.selectIntoEnabled=true;
-- !query schema
struct<key:string,value:string>
-- !query output
spark.sql.scripting.selectIntoEnabled	true

-- =============================================================================
-- SECTION B: BASIC USAGE
-- =============================================================================

-- =============================================================================
-- Test 5: Single column into single variable
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
-- Test 6: Multiple columns into multiple variables
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
-- Test 7: Expressions into variables
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
-- Test 8: SELECT * with INTO
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
-- Test 9: Column aliases with INTO
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

-- =============================================================================
-- SECTION C: TYPE CASTING
-- =============================================================================

-- =============================================================================
-- Test 10: Implicit upcast (INT to BIGINT)
-- =============================================================================

-- !query
BEGIN
  DECLARE big_var BIGINT;
  SELECT id INTO big_var FROM tbl_view WHERE id = 10;
  SELECT big_var, typeof(big_var);
END;
-- !query schema
struct<big_var:bigint,typeof(big_var):string>
-- !query output
10	bigint

-- =============================================================================
-- Test 11: Implicit downcast with precision loss (DOUBLE to INT)
-- =============================================================================

-- !query
BEGIN
  DECLARE int_var INT;
  SELECT CAST(1.9 AS DOUBLE) AS dval INTO int_var;
  SELECT int_var, typeof(int_var);
END;
-- !query schema
struct<int_var:int,typeof(int_var):string>
-- !query output
1	int

-- =============================================================================
-- Test 12: Invalid cast error (STRING to INT)
-- =============================================================================

-- !query
BEGIN
  DECLARE int_var INT;
  SELECT name INTO int_var FROM tbl_view WHERE id = 10;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.SparkRuntimeException
{
  "errorClass" : "INVALID_VARIABLE_TYPE_FOR_ASSIGNMENT",
  "sqlState" : "42821",
  "messageParameters" : {
    "sqlValue" : "name1",
    "sqlValueType" : "STRING",
    "varName" : "`int_var`",
    "varType" : "INT"
  }
}

-- =============================================================================
-- Test 13: Numeric overflow (large DOUBLE to INT)
-- =============================================================================

-- !query
BEGIN
  DECLARE int_var INT;
  SELECT CAST(1.0E10 AS DOUBLE) AS bigval INTO int_var;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.SparkArithmeticException
{
  "errorClass" : "CAST_OVERFLOW",
  "sqlState" : "22003",
  "messageParameters" : {
    "ansiConfig" : "\"spark.sql.ansi.enabled\"",
    "sourceType" : "\"DOUBLE\"",
    "targetType" : "\"INT\"",
    "value" : "1.0E10D"
  }
}

-- =============================================================================
-- Test 14: Numeric overflow (INT to SMALLINT)
-- =============================================================================

-- !query
BEGIN
  DECLARE small_var SMALLINT;
  SELECT 100000 AS bigint INTO small_var;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.SparkArithmeticException
{
  "errorClass" : "CAST_OVERFLOW",
  "sqlState" : "22003",
  "messageParameters" : {
    "ansiConfig" : "\"spark.sql.ansi.enabled\"",
    "sourceType" : "\"INT\"",
    "targetType" : "\"SMALLINT\"",
    "value" : "100000"
  }
}

-- =============================================================================
-- SECTION D: VARIABLE SCOPING
-- =============================================================================

-- =============================================================================
-- Test 15: Local and session variables combined
-- =============================================================================

-- !query
BEGIN
  DECLARE local_var INT;
  DECLARE VARIABLE session_var STRING DEFAULT 'initial';
  SELECT id, name INTO local_var, session_var FROM tbl_view WHERE id = 10;
  SELECT local_var, session_var;
END;
-- !query schema
struct<local_var:int,session_var:string>
-- !query output
10	name1

-- Verify session variable persists
-- !query
SELECT session_var;
-- !query schema
struct<session_var:string>
-- !query output
name1

-- =============================================================================
-- Test 16: Qualified variable names (system.session.varname)
-- =============================================================================

-- !query
BEGIN
  DECLARE myvar INT DEFAULT 99;
  SELECT id INTO system.session.myvar FROM tbl_view WHERE id = 20;
  SELECT system.session.myvar;
END;
-- !query schema
struct<myvar:int>
-- !query output
20

-- =============================================================================
-- Test 17: IDENTIFIER clause for variable names
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  DECLARE v2 STRING;
  SELECT id, name INTO IDENTIFIER('v1'), IDENTIFIER('v2') FROM tbl_view WHERE id = 30;
  SELECT v1, v2;
END;
-- !query schema
struct<v1:int,v2:string>
-- !query output
30	name3

-- =============================================================================
-- SECTION E: STRUCT HANDLING
-- =============================================================================

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
-- Test 20: Struct field count mismatch (error)
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
-- SECTION F: ZERO ROWS BEHAVIOR (NO DATA CONDITION)
-- =============================================================================

-- =============================================================================
-- Test 21: SELECT INTO raises NO DATA (SQLSTATE 02000) on zero rows - unhandled
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  SET VAR v1 = 42;
  SELECT id INTO v1 FROM tbl_view WHERE 1=0;
  SELECT v1;  -- Should not execute
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_NO_DATA",
  "sqlState" : "02000"
}

-- =============================================================================
-- Test 22: SELECT INTO with CONTINUE HANDLER for NOT FOUND
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  DECLARE no_data_flag BOOLEAN DEFAULT false;
  
  -- Handler catches NO DATA condition
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET no_data_flag = true;
  
  SET VAR v1 = 42;
  SELECT id INTO v1 FROM tbl_view WHERE 1=0;  -- Triggers handler
  
  -- Execution continues, variables unchanged, flag set
  SELECT v1, no_data_flag;
END;
-- !query schema
struct<v1:int,no_data_flag:boolean>
-- !query output
42	true

-- =============================================================================
-- Test 23: SELECT INTO with CONTINUE HANDLER for SQLSTATE '02000'
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  DECLARE v2 STRING;
  DECLARE found BOOLEAN DEFAULT true;
  
  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET found = false;
  
  SET VAR v1 = 99;
  SET VAR v2 = 'initial';
  SELECT id, name INTO v1, v2 FROM tbl_view WHERE id = 999;  -- Triggers handler
  
  SELECT v1, v2, found;
END;
-- !query schema
struct<v1:int,v2:string,found:boolean>
-- !query output
99	initial	false

-- =============================================================================
-- Test 24: SELECT INTO with EXIT HANDLER for NOT FOUND
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  
  -- EXIT handler terminates the block
  DECLARE EXIT HANDLER FOR NOT FOUND BEGIN
    VALUES ('Handler executed - no data found');
  END;
  
  SET VAR v1 = 100;
  SELECT id INTO v1 FROM tbl_view WHERE FALSE;  -- Triggers handler, exits block
  
  VALUES ('This should not execute');
END;
-- !query schema
struct<col1:string>
-- !query output
Handler executed - no data found

-- =============================================================================
-- Test 24b: SELECT INTO NO DATA with struct variable and handler
-- =============================================================================

-- !query
BEGIN
  DECLARE my_struct STRUCT<x: INT, y: STRING>;
  DECLARE handled BOOLEAN DEFAULT false;
  
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET handled = true;
  
  SET VAR my_struct = named_struct('x', 100, 'y', 'original');
  SELECT id, name INTO my_struct FROM tbl_view WHERE id < 0;  -- Triggers handler
  
  SELECT my_struct, handled;
END;
-- !query schema
struct<my_struct:struct<x:int,y:string>,handled:boolean>
-- !query output
{"x":100,"y":"original"}	true

-- =============================================================================
-- SECTION G: RESULT SET BEHAVIOR
-- =============================================================================

-- =============================================================================
-- Test 25: SELECT INTO does not return a result set
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  DECLARE v2 STRING;
  SELECT id, name FROM tbl_view WHERE id = 10;
  SELECT id INTO v1 FROM tbl_view WHERE id = 20;
  SELECT name INTO v2 FROM tbl_view WHERE id = 30;
  SELECT v1, v2;
END;
-- !query schema
struct<v1:int,v2:string>
-- !query output
20	name3

-- =============================================================================
-- SECTION H: ERROR CASES - CONTEXT/PLACEMENT
-- =============================================================================

-- =============================================================================
-- Test 26: SELECT INTO outside SQL script (not in BEGIN...END)
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
-- Test 27: SELECT INTO in subquery
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
-- Test 28: SELECT INTO in UNION (first branch)
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
-- Test 29: SELECT INTO in UNION (last branch)
-- =============================================================================

-- !query
BEGIN
  DECLARE v INT;
  SELECT id FROM tbl_view WHERE id = 10
  UNION
  SELECT id INTO v FROM tbl_view WHERE id = 20;
END;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.catalyst.ExtendedAnalysisException
{
  "errorClass" : "SELECT_INTO_ONLY_AT_TOP_LEVEL",
  "sqlState" : "42601"
}

-- =============================================================================
-- Test 30: SELECT INTO in INTERSECT
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
-- Test 31: SELECT INTO in EXCEPT
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
-- SECTION I: ERROR CASES - DATA ISSUES
-- =============================================================================

-- =============================================================================
-- Test 32: Too many variables (3 vars, 2 columns)
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
-- Test 33: Too few variables (1 var, 2 columns)
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
-- Test 34: Variable count mismatch with expressions
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
-- Test 35: Multiple rows returned (error)
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

-- Clean up
DROP VIEW tbl_view;
