-- Comprehensive test suite for SELECT INTO
-- Tests all error conditions and valid use cases

-- Setup test data
CREATE TEMPORARY VIEW tbl_view AS SELECT * FROM VALUES
  (10, 'name1'),
  (20, 'name2'),
  (30, 'name3')
AS tbl_view(id, name);

-- =============================================================================
-- ERROR CONDITION 5: SELECT INTO only allowed in SQL scripts
-- =============================================================================

-- !query
SELECT id INTO res_id FROM tbl_view WHERE id = 10;
-- !query schema
struct<>
-- !query output
org.apache.spark.sql.AnalysisException
{
  "errorClass" : "SELECT_INTO_NOT_IN_SQL_SCRIPT",
  "sqlState" : "42601"
}

-- =============================================================================
-- ERROR CONDITION 1: SELECT INTO not allowed in nested queries/subqueries
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
  "errorClass" : "SELECT_INTO_IN_NESTED_QUERY",
  "sqlState" : "42601"
}

-- =============================================================================
-- ERROR CONDITION 2: SELECT INTO not allowed in set operations
-- =============================================================================

-- Test with UNION
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
  "errorClass" : "SELECT_INTO_IN_SET_OPERATION",
  "sqlState" : "42601"
}

-- Test with INTERSECT
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
  "errorClass" : "SELECT_INTO_IN_SET_OPERATION",
  "sqlState" : "42601"
}

-- Test with EXCEPT
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
  "errorClass" : "SELECT_INTO_IN_SET_OPERATION",
  "sqlState" : "42601"
}

-- =============================================================================
-- ERROR CONDITION 3: Variable count must match column count
-- =============================================================================

-- Too many variables (3 vars, 2 columns)
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

-- Too few variables (1 var, 2 columns)
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

-- Cardinality mismatch with expressions
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
-- ERROR CONDITION 4: Struct field count must match column count
-- =============================================================================

-- Struct with wrong number of fields (struct has 2 fields, query has 3 columns)
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
-- ERROR CONDITION 6: More than one row returned
-- =============================================================================

-- !query
BEGIN
  DECLARE v1 INT;
  -- Query returns all rows (3 rows)
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
-- VALID CASES
-- =============================================================================

-- Single column into single variable
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

-- Multiple columns into multiple variables
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

-- Expressions into variables
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

-- Struct unpacking: Single struct variable with multiple columns
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

-- Struct unpacking: Verify field values are accessible
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
-- ZERO ROWS BEHAVIOR: Variables remain unchanged
-- =============================================================================

-- Multiple variables with initial values
-- !query
BEGIN
  DECLARE v1 INT;
  DECLARE v2 STRING;
  SET VAR v1 = 99;
  SET VAR v2 = 'initial';
  SELECT v1, v2;

  -- Query returns zero rows
  SELECT id, name INTO v1, v2 FROM tbl_view WHERE id = 999;

  -- Variables should still have original values
  SELECT v1, v2;
END;
-- !query schema
struct<v1:int,v2:string>
-- !query output
99	initial
99	initial

-- Single variable with initial value
-- !query
BEGIN
  DECLARE v1 INT;
  SET VAR v1 = 42;
  SELECT v1;

  -- Query returns zero rows
  SELECT id INTO v1 FROM tbl_view WHERE 1=0;

  -- v1 should still be 42
  SELECT v1;
END;
-- !query schema
struct<v1:int>
-- !query output
42
42

-- Variables with NULL initial values remain NULL
-- !query
BEGIN
  DECLARE v1 INT;
  -- v1 starts as NULL
  SELECT v1;

  -- Query returns zero rows
  SELECT id INTO v1 FROM tbl_view WHERE FALSE;

  -- v1 should still be NULL
  SELECT v1;
END;
-- !query schema
struct<v1:int>
-- !query output
NULL
NULL

-- Struct variable with zero rows
-- !query
BEGIN
  DECLARE my_struct STRUCT<x: INT, y: STRING>;
  SET VAR my_struct = named_struct('x', 100, 'y', 'original');
  SELECT my_struct;

  -- Query returns zero rows
  SELECT id, name INTO my_struct FROM tbl_view WHERE id < 0;

  -- Struct should still have original values
  SELECT my_struct;
END;
-- !query schema
struct<my_struct:struct<x:int,y:string>>
-- !query output
{"x":100,"y":"original"}
{"x":100,"y":"original"}

-- =============================================================================
-- EDGE CASES
-- =============================================================================

-- SELECT * with INTO
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

-- Column aliases don't affect INTO
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
