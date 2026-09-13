-------------------------------------------------------------------------------------------------
-- Ths testfile tests the ability to templatize identifiers such as table and column names in SQL
-- Common patterns are to to use variable substitution or parameter markers (test in another file)
--------------------------------------------------------------------------------------------------

-- Sanity test variable substitution
SET hivevar:colname = 'c';
SELECT IDENTIFIER(${colname} || '_1') FROM VALUES(1) AS T(c_1);

-- Column references
SELECT IDENTIFIER('c1') FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER('t.c1') FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER('`t`.c1') FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER('`c 1`') FROM VALUES(1) AS T(`c 1`);
SELECT IDENTIFIER('``') FROM VALUES(1) AS T(``);
SELECT IDENTIFIER('c' || '1') FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER(concat('a', 'b')) FROM VALUES(1) AS T(ab);

-- Table references
CREATE SCHEMA IF NOT EXISTS s;
CREATE TABLE s.tab(c1 INT) USING CSV;
USE SCHEMA s;

INSERT INTO IDENTIFIER('ta' || 'b') VALUES(1);
DELETE FROM IDENTIFIER('ta' || 'b') WHERE 1=0;
UPDATE IDENTIFIER('ta' || 'b') SET c1 = 2;
MERGE INTO IDENTIFIER('ta' || 'b') AS t USING IDENTIFIER('ta' || 'b') AS s ON s.c1 = t.c1
  WHEN MATCHED THEN UPDATE SET c1 = 3;
SELECT * FROM IDENTIFIER('tab');
SELECT * FROM IDENTIFIER('s.tab');
SELECT * FROM IDENTIFIER('`s`.`tab`');
SELECT * FROM IDENTIFIER('t' || 'a' || 'b');
SELECT * FROM IDENTIFIER(concat('t', 'ab'));

USE SCHEMA default;
DROP TABLE s.tab;
DROP SCHEMA s;

-- Function reference
SELECT IDENTIFIER('COAL' || 'ESCE')(NULL, 1);
SELECT IDENTIFIER(concat('COAL', 'ESCE'))(NULL, 1);
SELECT IDENTIFIER('abs')(c1) FROM VALUES(-1) AS T(c1);
SELECT * FROM IDENTIFIER('ra' || 'nge')(0, 1);

VALUES(IDENTIFIER(abs(1)));
SELECT * FROM IDENTIFIER(nullif('a', 'a'));

-- Function resolution while computing an identifier name
SELECT IDENTIFIER(max('c1')) FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER(array_join(transform(array('c', '1'), element -> element), ''))
FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER(rand()) FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER(row_number() OVER ()) FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER(row_number() OVER (ORDER BY 'x')) FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER(explode(array('c1'))) FROM VALUES(1) AS T(c1);
SELECT * FROM IDENTIFIER(max('identifier_function_table'));
SELECT * FROM IDENTIFIER(row_number() OVER (ORDER BY 'x'));
SELECT * FROM IDENTIFIER(
  array_join(transform(array('identifier', '_function_table'), element -> element), '')
);

CREATE TEMPORARY FUNCTION identifier_name()
RETURNS STRING
RETURN 'c1';
SELECT IDENTIFIER(identifier_name()) FROM VALUES(1) AS T(c1);
DROP TEMPORARY FUNCTION identifier_name;

CREATE FUNCTION persistent_identifier_name()
RETURNS STRING
RETURN 'c1';
SELECT IDENTIFIER(persistent_identifier_name()) FROM VALUES(1) AS T(c1);
DROP FUNCTION persistent_identifier_name;

CREATE FUNCTION persistent_identifier_function(value INT)
RETURNS INT
RETURN value + 1;
SELECT IDENTIFIER(concat('persistent_identifier_', 'function'))(1);
DROP FUNCTION persistent_identifier_function;

CREATE FUNCTION persistent_identifier_base_name()
RETURNS STRING
RETURN 'c1';
CREATE FUNCTION persistent_identifier_nested_name()
RETURNS STRING
RETURN persistent_identifier_base_name();
SELECT IDENTIFIER(persistent_identifier_nested_name()) FROM VALUES(1) AS T(c1);
DROP FUNCTION persistent_identifier_nested_name;
DROP FUNCTION persistent_identifier_base_name;

CREATE TEMPORARY FUNCTION identifier_relation_name()
RETURNS STRING
RETURN 'identifier_function_table';
CREATE TABLE identifier_function_table(c1 INT) USING csv;
SELECT * FROM IDENTIFIER(identifier_relation_name());
CREATE TEMPORARY FUNCTION identifier_relation_count()
RETURNS BIGINT
RETURN SELECT count(*) FROM IDENTIFIER(concat('identifier_function_', 'table'));
SELECT identifier_relation_count();
DROP TEMPORARY FUNCTION identifier_relation_count;
CREATE FUNCTION persistent_identifier_relation_name()
RETURNS STRING
RETURN 'identifier_function_table';
SELECT * FROM IDENTIFIER(persistent_identifier_relation_name());
DROP FUNCTION persistent_identifier_relation_name;
DROP TABLE identifier_function_table;
DROP TEMPORARY FUNCTION identifier_relation_name;

-- Table DDL
CREATE TABLE IDENTIFIER('tab')(c1 INT) USING CSV;
DROP TABLE IF EXISTS IDENTIFIER('ta' || 'b');

CREATE SCHEMA identifier_clauses;
USE identifier_clauses;
CREATE TABLE IDENTIFIER('ta' || 'b')(c1 INT) USING CSV;
DROP TABLE IF EXISTS IDENTIFIER('identifier_clauses.' || 'tab');
CREATE TABLE IDENTIFIER('identifier_clauses.' || 'tab')(c1 INT) USING CSV;
REPLACE TABLE IDENTIFIER('identifier_clauses.' || 'tab')(c1 INT) USING CSV;
CACHE TABLE IDENTIFIER('ta' || 'b');
UNCACHE TABLE IDENTIFIER('ta' || 'b');
DROP TABLE IF EXISTS IDENTIFIER('ta' || 'b');
USE default;
DROP SCHEMA identifier_clauses;

CREATE TABLE tab(c1 INT) USING CSV;
INSERT INTO tab VALUES (1);
SELECT c1 FROM tab;
DESCRIBE IDENTIFIER('ta' || 'b');
ANALYZE TABLE IDENTIFIER('ta' || 'b') COMPUTE STATISTICS;
ALTER TABLE IDENTIFIER('ta' || 'b') ADD COLUMN c2 INT;
SHOW TBLPROPERTIES IDENTIFIER('ta' || 'b');
SHOW COLUMNS FROM IDENTIFIER('ta' || 'b');
COMMENT ON TABLE IDENTIFIER('ta' || 'b') IS 'hello';
REFRESH TABLE IDENTIFIER('ta' || 'b');
REPAIR TABLE IDENTIFIER('ta' || 'b');
TRUNCATE TABLE IDENTIFIER('ta' || 'b');
DROP TABLE IF EXISTS tab;

-- View
CREATE OR REPLACE VIEW IDENTIFIER('v')(c1) AS VALUES(1);
SELECT * FROM v;
ALTER VIEW IDENTIFIER('v') AS VALUES(2);
DROP VIEW IDENTIFIER('v');
CREATE TEMPORARY VIEW IDENTIFIER('v')(c1) AS VALUES(1);
DROP VIEW IDENTIFIER('v');

-- Schema
CREATE SCHEMA IDENTIFIER('id' || 'ent');
ALTER SCHEMA IDENTIFIER('id' || 'ent') SET PROPERTIES (somekey = 'somevalue');
ALTER SCHEMA IDENTIFIER('id' || 'ent') SET LOCATION 'someloc';
COMMENT ON SCHEMA IDENTIFIER('id' || 'ent') IS 'some comment';
DESCRIBE SCHEMA IDENTIFIER('id' || 'ent');
SHOW TABLES IN IDENTIFIER('id' || 'ent');
SHOW TABLE EXTENDED IN IDENTIFIER('id' || 'ent') LIKE 'hello';
USE IDENTIFIER('id' || 'ent');
SHOW CURRENT SCHEMA;
USE SCHEMA IDENTIFIER('id' || 'ent');
USE SCHEMA default;
DROP SCHEMA IDENTIFIER('id' || 'ent');

-- Function
CREATE SCHEMA ident;
CREATE FUNCTION IDENTIFIER('ident.' || 'myDoubleAvg') AS 'test.org.apache.spark.sql.MyDoubleAvg';
DESCRIBE FUNCTION IDENTIFIER('ident.' || 'myDoubleAvg');
REFRESH FUNCTION IDENTIFIER('ident.' || 'myDoubleAvg');
DROP FUNCTION IDENTIFIER('ident.' || 'myDoubleAvg');
DROP SCHEMA ident;
CREATE TEMPORARY FUNCTION IDENTIFIER('my' || 'DoubleAvg') AS 'test.org.apache.spark.sql.MyDoubleAvg';
DROP TEMPORARY FUNCTION IDENTIFIER('my' || 'DoubleAvg');

-- IDENTIFIER + variable
DECLARE var = 'sometable';
CREATE TABLE IDENTIFIER(var)(c1 INT) USING CSV;

SET VAR var = 'c1';
SELECT IDENTIFIER(var) FROM VALUES(1) AS T(c1);

SET VAR var = 'some';
DROP TABLE IDENTIFIER(var || 'table');

-- Error conditions
SELECT IDENTIFIER('c 1') FROM VALUES(1) AS T(`c 1`);
SELECT IDENTIFIER('') FROM VALUES(1) AS T(``);
VALUES(IDENTIFIER(CAST(NULL AS STRING)));
VALUES(IDENTIFIER(1));
VALUES(IDENTIFIER(SUBSTR('HELLO', 1, RAND() + 1)));
SELECT `IDENTIFIER`('abs')(c1) FROM VALUES(-1) AS T(c1);

CREATE TABLE t(col1 INT);
CREATE TABLE identifier_name_source(name STRING) USING csv;
SELECT * FROM IDENTIFIER((SELECT 't'));
SELECT * FROM IDENTIFIER((SELECT max(name) FROM identifier_name_source));
SELECT * FROM IDENTIFIER(
  (SELECT 'x' FROM (SELECT 1) WHERE explode(array(1)) = 1)
);
SELECT * FROM (SELECT IDENTIFIER((SELECT 'col1')) FROM IDENTIFIER((SELECT 't')));
SELECT IDENTIFIER((SELECT 'col1')) FROM VALUES(1);
SELECT col1, IDENTIFIER((SELECT col1)) FROM VALUES(1);
SELECT IDENTIFIER((SELECT 'col1', 'col2')) FROM VALUES(1,2);
DROP TABLE identifier_name_source;
DROP TABLE t;

CREATE TABLE IDENTIFIER(1)(c1 INT) USING csv;
CREATE TABLE IDENTIFIER('a.b.c')(c1 INT) USING csv;
CREATE VIEW IDENTIFIER('a.b.c')(c1) AS VALUES(1);
DROP TABLE IDENTIFIER('a.b.c');
DROP VIEW IDENTIFIER('a.b.c');
COMMENT ON TABLE IDENTIFIER('a.b.c.d') IS 'hello';
VALUES(IDENTIFIER(1)());
VALUES(IDENTIFIER('a.b.c.d')());

CREATE TEMPORARY FUNCTION IDENTIFIER('default.my' || 'DoubleAvg') AS 'test.org.apache.spark.sql.MyDoubleAvg';
DROP TEMPORARY FUNCTION IDENTIFIER('default.my' || 'DoubleAvg');
CREATE TEMPORARY VIEW IDENTIFIER('default.v')(c1) AS VALUES(1);

-- SPARK-48273: Aggregation operation in statements using identifier clause for table name
create temporary view identifier('v1') as (select my_col from (values (1), (2), (1) as (my_col)) group by 1);
cache table identifier('t1') as (select my_col from (values (1), (2), (1) as (my_col)) group by 1);
create table identifier('t2') using csv as (select my_col from (values (1), (2), (1) as (my_col)) group by 1);
insert into identifier('t2') select my_col from (values (3) as (my_col)) group by 1;
drop view v1;
drop table t1;
drop table t2;

-- SPARK-46625: CTE reference with identifier clause and session variables
DECLARE agg = 'max';
DECLARE col = 'c1';
DECLARE tab = 'T';

WITH S(c1, c2) AS (VALUES(1, 2), (2, 3)),
     T(c1, c2) AS (VALUES ('a', 'b'), ('c', 'd'))
SELECT IDENTIFIER(agg)(IDENTIFIER(col)) FROM IDENTIFIER(tab);

WITH S(c1, c2) AS (VALUES(1, 2), (2, 3)),
     T(c1, c2) AS (VALUES ('a', 'b'), ('c', 'd'))
SELECT IDENTIFIER('max')(IDENTIFIER('c1')) FROM IDENTIFIER('T');

WITH ABC(c1, c2) AS (VALUES(1, 2), (2, 3))
SELECT IDENTIFIER('max')(IDENTIFIER('c1')) FROM IDENTIFIER('A' || 'BC');

SELECT row_number() OVER IDENTIFIER('x.win') FROM VALUES(1) AS T(c1) WINDOW win AS (ORDER BY c1);
SELECT T1.c1 FROM VALUES(1) AS T1(c1) JOIN VALUES(1) AS T2(c1) USING (IDENTIFIER('c1'));
SELECT IDENTIFIER('t').c1 FROM VALUES(1) AS T(c1);
SELECT map('a', 1).IDENTIFIER('a') FROM VALUES(1) AS T(c1);
SELECT named_struct('a', 1).IDENTIFIER('a') FROM VALUES(1) AS T(c1);
SELECT * FROM s.IDENTIFIER('tab');
SELECT * FROM IDENTIFIER('s').IDENTIFIER('tab');
SELECT * FROM IDENTIFIER('s').tab;
SELECT row_number() OVER IDENTIFIER('win') FROM VALUES(1) AS T(c1) WINDOW win AS (ORDER BY c1);
SELECT row_number() OVER win FROM VALUES(1) AS T(c1) WINDOW IDENTIFIER('win') AS (ORDER BY c1);
SELECT 1 AS IDENTIFIER('col1');
SELECT my_table.* FROM VALUES (1, 2) AS IDENTIFIER('my_table')(IDENTIFIER('c1'), IDENTIFIER('c2'));
WITH identifier('v')(identifier('c1')) AS (VALUES(1)) (SELECT c1 FROM v);
CREATE OR REPLACE VIEW v(IDENTIFIER('c1')) AS VALUES(1);
SELECT c1 FROM v;
DROP VIEW IF EXISTS v;
CREATE TABLE tab(IDENTIFIER('c1') INT) USING CSV;
INSERT INTO tab(IDENTIFIER('c1')) VALUES(1);
SELECT c1 FROM tab;
ALTER TABLE IDENTIFIER('tab') RENAME COLUMN IDENTIFIER('c1') TO IDENTIFIER('col1');
SELECT col1 FROM tab;
ALTER TABLE IDENTIFIER('tab') ADD COLUMN IDENTIFIER('c2') INT;
SELECT c2 FROM tab;
ALTER TABLE IDENTIFIER('tab') DROP COLUMN IDENTIFIER('c2');
ALTER TABLE IDENTIFIER('tab') RENAME TO IDENTIFIER('tab_renamed');
SELECT * FROM tab_renamed;
DROP TABLE IF EXISTS tab_renamed;
DROP TABLE IF EXISTS tab;

-- Error because qualified names are not allowed
CREATE TABLE test_col_with_dot(IDENTIFIER('`col.with.dot`') INT) USING CSV;
DROP TABLE IF EXISTS test_col_with_dot;
-- Identifier-lite: table alias with qualified name should error (table alias must be single)
SELECT * FROM VALUES (1, 2) AS IDENTIFIER('schema.table')(c1, c2);
-- Identifier-lite: column alias with qualified name should error (column alias must be single)
SELECT 1 AS IDENTIFIER('col1.col2');

-- Additional coverage: SHOW commands with identifier-lite
CREATE SCHEMA identifier_clause_test_schema;
USE identifier_clause_test_schema;
CREATE TABLE test_show(c1 INT, c2 STRING) USING CSV;
SHOW VIEWS IN IDENTIFIER('identifier_clause_test_schema');
SHOW PARTITIONS IDENTIFIER('test_show');
SHOW CREATE TABLE IDENTIFIER('test_show');
DROP TABLE test_show;

-- SET CATALOG with identifier-lite
-- SET CATALOG IDENTIFIER('spark_catalog');

-- DESCRIBE with different forms
CREATE TABLE test_desc(c1 INT) USING CSV;
DESCRIBE TABLE IDENTIFIER('test_desc');
DESCRIBE FORMATTED IDENTIFIER('test_desc');
DESCRIBE EXTENDED IDENTIFIER('test_desc');
DESC IDENTIFIER('test_desc');
DROP TABLE test_desc;

-- COMMENT ON COLUMN with identifier-lite
CREATE TABLE test_comment(c1 INT, c2 STRING) USING CSV;
COMMENT ON TABLE IDENTIFIER('test_comment') IS 'table comment';
ALTER TABLE test_comment ALTER COLUMN IDENTIFIER('c1') COMMENT 'column comment';
COMMENT ON TABLE IDENTIFIER('test_comment') COLUMN (c1 IS 'c1 comment', c2 IS 'c2 comment');
DROP TABLE test_comment;

-- Additional identifier tests with qualified table names in various commands
CREATE TABLE identifier_clause_test_schema.test_table(c1 INT) USING CSV;
ANALYZE TABLE IDENTIFIER('identifier_clause_test_schema.test_table') COMPUTE STATISTICS;
REFRESH TABLE IDENTIFIER('identifier_clause_test_schema.test_table');
DESCRIBE IDENTIFIER('identifier_clause_test_schema.test_table');
SHOW COLUMNS FROM IDENTIFIER('identifier_clause_test_schema.test_table');
DROP TABLE IDENTIFIER('identifier_clause_test_schema.test_table');

-- Session variables with identifier-lite
DECLARE IDENTIFIER('my_var') = 'value';
SET VAR IDENTIFIER('my_var') = 'new_value';
SELECT IDENTIFIER('my_var');
DROP TEMPORARY VARIABLE IDENTIFIER('my_var');

-- SQL UDF with identifier-lite in parameter names and return statement
CREATE TEMPORARY FUNCTION test_udf(IDENTIFIER('param1') INT, IDENTIFIER('param2') STRING)
RETURNS INT
RETURN IDENTIFIER('param1') + length(IDENTIFIER('param2'));

SELECT test_udf(5, 'hello');
DROP TEMPORARY FUNCTION test_udf;

-- SQL UDF with table return type using identifier-lite
CREATE TEMPORARY FUNCTION test_table_udf(IDENTIFIER('input_val') INT)
RETURNS TABLE(IDENTIFIER('col1') INT, IDENTIFIER('col2') STRING)
RETURN SELECT IDENTIFIER('input_val'), 'result';

SELECT * FROM test_table_udf(42);
DROP TEMPORARY FUNCTION test_table_udf;

-- Integration tests: Combining parameter markers, string coalescing, and IDENTIFIER
-- These tests demonstrate the power of combining IDENTIFIER with parameters

-- Test 1: IDENTIFIER with parameter marker for table name
EXECUTE IMMEDIATE 'SELECT IDENTIFIER(:tab \'b\').c1 FROM VALUES(1) AS tab(c1)' USING 'ta' AS tab;

-- Test 2: IDENTIFIER with string coalescing for column name
EXECUTE IMMEDIATE 'SELECT IDENTIFIER(:col1 ''.c2'') FROM VALUES(named_struct(''c2'', 42)) AS T(c1)'
  USING 'c1' AS col1;

-- Test 3: IDENTIFIER with parameter and string literal coalescing for qualified table name
CREATE TABLE integration_test(c1 INT, c2 STRING) USING CSV;
INSERT INTO integration_test VALUES (1, 'a'), (2, 'b');
EXECUTE IMMEDIATE 'SELECT * FROM IDENTIFIER(:schema ''.'' :table) ORDER BY ALL'
  USING 'identifier_clause_test_schema' AS schema, 'integration_test' AS table;

-- Test 4: IDENTIFIER in column reference with parameter and string coalescing
EXECUTE IMMEDIATE 'SELECT IDENTIFIER(:prefix ''1''), IDENTIFIER(:prefix ''2'') FROM integration_test ORDER BY ALL'
  USING 'c' AS prefix;

-- Test 5: IDENTIFIER in WHERE clause with parameters
EXECUTE IMMEDIATE 'SELECT * FROM integration_test WHERE IDENTIFIER(:col) = :val'
  USING 'c1' AS col, 1 AS val;

-- Test 6: IDENTIFIER in JOIN with parameters for table and column names
CREATE TABLE integration_test2(c1 INT, c3 STRING) USING CSV;
INSERT INTO integration_test2 VALUES (1, 'x'), (2, 'y');
EXECUTE IMMEDIATE 'SELECT t1.*, t2.* FROM IDENTIFIER(:t1) t1 JOIN IDENTIFIER(:t2) t2 USING (IDENTIFIER(:col)) ORDER BY ALL'
  USING 'integration_test' AS t1, 'integration_test2' AS t2, 'c1' AS col;

-- Test 7: IDENTIFIER in window function with parameter for partition column
EXECUTE IMMEDIATE
  'SELECT IDENTIFIER(:col1), IDENTIFIER(:col2), row_number() OVER (PARTITION BY IDENTIFIER(:part) ORDER BY IDENTIFIER(:ord)) as rn FROM integration_test'
  USING 'c1' AS col1, 'c2' AS col2, 'c2' AS part, 'c1' AS ord;

-- Test 8: IDENTIFIER in aggregate function with string coalescing
EXECUTE IMMEDIATE 'SELECT IDENTIFIER(:prefix ''2''), IDENTIFIER(:agg)(IDENTIFIER(:col)) FROM integration_test GROUP BY IDENTIFIER(:prefix ''2'') ORDER BY ALL'
  USING 'c' AS prefix, 'count' AS agg, 'c1' AS col;

-- Test 9: IDENTIFIER in ORDER BY with multiple parameters
EXECUTE IMMEDIATE 'SELECT * FROM integration_test ORDER BY IDENTIFIER(:col1) DESC, IDENTIFIER(:col2)'
  USING 'c1' AS col1, 'c2' AS col2;

-- Test 10: IDENTIFIER in INSERT with parameter for column name
EXECUTE IMMEDIATE 'INSERT INTO integration_test(IDENTIFIER(:col1), IDENTIFIER(:col2)) VALUES (:val1, :val2)'
  USING 'c1' AS col1, 'c2' AS col2, 3 AS val1, 'c' AS val2;

-- Test 11: Complex - IDENTIFIER with nested string operations
EXECUTE IMMEDIATE 'SELECT IDENTIFIER(concat(:schema, ''.'', :table, ''.c1'')) FROM VALUES(named_struct(''c1'', 100)) AS IDENTIFIER(:alias)(IDENTIFIER(:schema ''.'' :table))'
  USING 'identifier_clause_test_schema' AS schema, 'my_table' AS table, 't' AS alias;

-- Test 12: IDENTIFIER in CTE name with parameter
EXECUTE IMMEDIATE 'WITH IDENTIFIER(:cte_name)(c1) AS (VALUES(1)) SELECT c1 FROM IDENTIFIER(:cte_name)'
  USING 'my_cte' AS cte_name;

-- Test 13: IDENTIFIER in view name with parameter
EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY VIEW IDENTIFIER(:view_name)(IDENTIFIER(:col_name)) AS VALUES(1)'
  USING 'test_view' AS view_name, 'test_col' AS col_name;
EXECUTE IMMEDIATE 'SELECT IDENTIFIER(:col) FROM IDENTIFIER(:view)'
  USING 'test_col' AS col, 'test_view' AS view;
DROP VIEW test_view;

-- Test 14: IDENTIFIER in ALTER TABLE with parameters
EXECUTE IMMEDIATE 'ALTER TABLE IDENTIFIER(:tab) ADD COLUMN IDENTIFIER(:new_col) INT'
  USING 'integration_test' AS tab, 'c4' AS new_col;
EXECUTE IMMEDIATE 'ALTER TABLE IDENTIFIER(:tab) RENAME COLUMN IDENTIFIER(:old_col) TO IDENTIFIER(:new_col)'
  USING 'integration_test' AS tab, 'c4' AS old_col, 'c5' AS new_col;

-- Test 15: IDENTIFIER with dereference using parameters
EXECUTE IMMEDIATE 'SELECT map(:key, :val).IDENTIFIER(:key) AS result'
  USING 'mykey' AS key, 42 AS val;

-- Test 16: IDENTIFIER in table alias with string coalescing
EXECUTE IMMEDIATE 'SELECT IDENTIFIER(:alias ''.c1'') FROM integration_test AS IDENTIFIER(:alias) ORDER BY ALL'
  USING 't' AS alias;

-- Test 17: Multiple IDENTIFIER clauses with different parameter combinations
EXECUTE IMMEDIATE
  'SELECT IDENTIFIER(:col1), IDENTIFIER(:p ''2'') FROM IDENTIFIER(:schema ''.'' :tab) WHERE IDENTIFIER(:col1) > 0 ORDER BY IDENTIFIER(:p ''1'')'
  USING 'c1' AS col1, 'c' AS p, 'identifier_clause_test_schema' AS schema, 'integration_test' AS tab;

-- Test 19: IDENTIFIER with qualified name coalescing for schema.table.column pattern
-- This should work for multi-part identifiers
EXECUTE IMMEDIATE 'SELECT * FROM IDENTIFIER(:schema ''.'' :table) WHERE IDENTIFIER(concat(:tab_alias, ''.c1'')) > 0 ORDER BY ALL'
  USING 'identifier_clause_test_schema' AS schema, 'integration_test' AS table, 'integration_test' AS tab_alias;

-- Test 20: Error case - IDENTIFIER with too many parts from parameter coalescing
-- This should error as column alias must be single identifier
EXECUTE IMMEDIATE 'SELECT 1 AS IDENTIFIER(:schema ''.'' :col)'
  USING 'identifier_clause_test_schema' AS schema, 'col1' AS col;

-- Cleanup
DROP TABLE integration_test;
DROP TABLE integration_test2;

-- LATERAL VIEW with IDENTIFIER() for table and column names
CREATE TABLE lateral_test(arr ARRAY<INT>) USING PARQUET;
INSERT INTO lateral_test VALUES (array(1, 2, 3));
SELECT * FROM lateral_test LATERAL VIEW explode(arr) IDENTIFIER('tbl') AS IDENTIFIER('col') ORDER BY ALL;
SELECT * FROM lateral_test LATERAL VIEW OUTER explode(arr) IDENTIFIER('my_table') AS IDENTIFIER('my_col') ORDER BY ALL;
DROP TABLE lateral_test;

-- UNPIVOT with IDENTIFIER() for value column alias
CREATE TABLE unpivot_test(id INT, a INT, b INT, c INT) USING CSV;
INSERT INTO unpivot_test VALUES (1, 10, 20, 30);
SELECT * FROM unpivot_test UNPIVOT (val FOR col IN (a AS IDENTIFIER('col_a'), b AS IDENTIFIER('col_b'))) ORDER BY ALL;
SELECT * FROM unpivot_test UNPIVOT ((v1, v2) FOR col IN ((a, b) AS IDENTIFIER('cols_ab'), (b, c) AS IDENTIFIER('cols_bc'))) ORDER BY ALL;
DROP TABLE unpivot_test;

-- DESCRIBE column with IDENTIFIER()
CREATE TABLE describe_col_test(c1 INT, c2 STRING, c3 DOUBLE) USING CSV;
DESCRIBE describe_col_test IDENTIFIER('c1');
DESCRIBE describe_col_test IDENTIFIER('c2');
DROP TABLE describe_col_test;

-- All the following tests fail because they are not about "true" identifiers

-- This should fail - named parameters don't support IDENTIFIER()
SELECT :IDENTIFIER('param1') FROM VALUES(1) AS T(c1);

-- Hint names use simpleIdentifier - these should fail
CREATE TABLE hint_test(c1 INT, c2 INT) USING CSV;
INSERT INTO hint_test VALUES (1, 2), (3, 4);
SELECT /*+ IDENTIFIER('BROADCAST')(hint_test) */ * FROM hint_test;
SELECT /*+ IDENTIFIER('MERGE')(hint_test) */ * FROM hint_test;
DROP TABLE hint_test;

-- These should fail - function scope doesn't support IDENTIFIER()
SHOW IDENTIFIER('USER') FUNCTIONS;

-- EXTRACT field name uses simpleIdentifier - should fail
SELECT EXTRACT(IDENTIFIER('YEAR') FROM DATE'2024-01-15');

-- TIMESTAMPADD unit is a token, not identifier - should fail
SELECT TIMESTAMPADD(IDENTIFIER('YEAR'), 1, DATE'2024-01-15');

DROP SCHEMA identifier_clause_test_schema;

-- A variable read only by a temporary view's IDENTIFIER clause is still a variable the view refers
-- to, so it stays resolvable when the stored view text is analyzed again.
CREATE OR REPLACE TEMPORARY VIEW identifier_var_target AS SELECT 1 AS c1;
DECLARE OR REPLACE VARIABLE identifier_var_name STRING DEFAULT 'identifier_var_target';
CREATE OR REPLACE TEMPORARY VIEW identifier_var_view AS
SELECT * FROM IDENTIFIER(identifier_var_name);
SELECT count(*) AS row_count FROM identifier_var_view;
DROP VIEW identifier_var_view;
DROP VIEW identifier_var_target;
DROP TEMPORARY VARIABLE identifier_var_name;

-- A permanent view must still reject a temporary variable read via an IDENTIFIER clause.
CREATE TEMPORARY VIEW identifier_perm_target AS SELECT 1 AS c1;
DECLARE OR REPLACE VARIABLE identifier_perm_name STRING DEFAULT 'identifier_perm_target';
CREATE VIEW identifier_perm_view AS SELECT * FROM IDENTIFIER(identifier_perm_name);
DROP VIEW identifier_perm_target;
DROP TEMPORARY VARIABLE identifier_perm_name;

-- A permanent ALTER VIEW must also reject a temporary variable read via an IDENTIFIER clause.
-- The IDENTIFIER target is a permanent table so the variable is the only temporary object.
CREATE SCHEMA identifier_alter_schema;
USE identifier_alter_schema;
CREATE TABLE identifier_alter_target (c1 INT) USING parquet;
DECLARE OR REPLACE VARIABLE identifier_alter_name STRING DEFAULT 'identifier_alter_target';
CREATE VIEW identifier_alter_view AS SELECT 1 AS c1;
ALTER VIEW identifier_alter_view AS SELECT * FROM IDENTIFIER(identifier_alter_name);
SELECT * FROM identifier_alter_view;
DROP VIEW identifier_alter_view;
DROP TABLE identifier_alter_target;
DROP TEMPORARY VARIABLE identifier_alter_name;
USE default;
DROP SCHEMA identifier_alter_schema;

-- ALTER VIEW whose TARGET is picked by a variable, with a body that has no temp dependency, must
-- still succeed: the variable is not a dependency of the view definition.
CREATE SCHEMA ivt_schema;
USE ivt_schema;
CREATE VIEW ivt_target AS SELECT 1 AS c1;
DECLARE OR REPLACE VARIABLE ivt_name STRING DEFAULT 'ivt_target';
ALTER VIEW IDENTIFIER(ivt_name) AS SELECT 2 AS c1;
SELECT * FROM ivt_target;
DROP VIEW ivt_target;
DROP TEMPORARY VARIABLE ivt_name;
USE default;
DROP SCHEMA ivt_schema;

-- A temporary view whose body reads a variable via an IDENTIFIER clause in expression position
-- (here as a column name). Unlike the table-position case above, this identifier expression
-- resolves to a bare variable reference, so recording it requires visiting the root of the
-- expression tree. If it is not recorded, the variable is missing from the stored view text and
-- reading the view back fails.
DECLARE OR REPLACE VARIABLE identifier_expr_col STRING DEFAULT 'c1';
CREATE OR REPLACE TEMPORARY VIEW identifier_expr_view AS
SELECT IDENTIFIER(identifier_expr_col) AS x FROM VALUES(1) AS t(c1);
SELECT * FROM identifier_expr_view;
DROP VIEW identifier_expr_view;
DROP TEMPORARY VARIABLE identifier_expr_col;

-- A GLOBAL TEMPORARY VIEW reads a variable via an IDENTIFIER clause the same way a session-local
-- temporary view does, so the variable stays resolvable when the stored view text is analyzed again.
DECLARE OR REPLACE VARIABLE identifier_gtv_col STRING DEFAULT 'c1';
CREATE OR REPLACE GLOBAL TEMPORARY VIEW identifier_gtv_view AS
SELECT IDENTIFIER(identifier_gtv_col) AS x FROM VALUES(1) AS t(c1);
SELECT * FROM global_temp.identifier_gtv_view;
DROP VIEW global_temp.identifier_gtv_view;
DROP TEMPORARY VARIABLE identifier_gtv_col;

-- A variable used only to supply a view's NAME via an IDENTIFIER clause is not part of the view
-- definition, so it must not be recorded as a referred variable of the view.
DECLARE OR REPLACE VARIABLE identifier_view_name STRING DEFAULT 'identifier_named_view';
CREATE OR REPLACE TEMPORARY VIEW IDENTIFIER(identifier_view_name) AS SELECT 1 AS c1;
SELECT * FROM identifier_named_view;
DROP VIEW identifier_named_view;
DROP TEMPORARY VARIABLE identifier_view_name;

-- When both the NAME and the BODY use an IDENTIFIER clause, only the body variable is a dependency
-- of the view; the name variable must not be recorded.
DECLARE OR REPLACE VARIABLE identifier_name_part STRING DEFAULT 'identifier_named_view2';
DECLARE OR REPLACE VARIABLE identifier_body_part STRING DEFAULT 'c1';
CREATE OR REPLACE TEMPORARY VIEW IDENTIFIER(identifier_name_part) AS
SELECT IDENTIFIER(identifier_body_part) AS x FROM VALUES(1) AS t(c1);
SELECT * FROM identifier_named_view2;
DROP VIEW identifier_named_view2;
DROP TEMPORARY VARIABLE identifier_name_part;
DROP TEMPORARY VARIABLE identifier_body_part;

-- ALTER VIEW honors `spark.sql.legacy.allowSessionVariableInPersistedView` just like CREATE VIEW:
-- with it set, the DDL is allowed instead of rejected (the legacy permissive behavior). The flag
-- does not persist the variable into the stored view, so the view is not guaranteed to resolve in a
-- fresh session; the default (tested by `identifier_alter_view` above) rejects it outright.
SET spark.sql.legacy.allowSessionVariableInPersistedView=true;
DECLARE OR REPLACE VARIABLE identifier_p2a_col STRING DEFAULT 'c1';
CREATE VIEW identifier_p2a_view AS SELECT 1 AS c1;
ALTER VIEW identifier_p2a_view AS SELECT IDENTIFIER(identifier_p2a_col) FROM VALUES(1) AS t(c1);
DROP VIEW identifier_p2a_view;
DROP TEMPORARY VARIABLE identifier_p2a_col;
SET spark.sql.legacy.allowSessionVariableInPersistedView=false;

-- Nested temporary views lock in the `withAnalysisContext` accumulator reset: creating the outer
-- view (which selects the inner view) resolves the inner view along the way, and the inner view's
-- IDENTIFIER-clause variable must be attributed to the inner view only -- it must never be recorded
-- as a referred variable of the outer view. The inner `CreateViewCommand` records
-- `identifier_nested_col`; the outer `CreateViewCommand` must record nothing.
DECLARE OR REPLACE VARIABLE identifier_nested_col STRING DEFAULT 'c1';
CREATE OR REPLACE TEMPORARY VIEW identifier_nested_inner AS
SELECT IDENTIFIER(identifier_nested_col) AS x FROM VALUES(1) AS t(c1);
CREATE OR REPLACE TEMPORARY VIEW identifier_nested_outer AS
SELECT x FROM identifier_nested_inner;
SELECT * FROM identifier_nested_outer;
DROP VIEW identifier_nested_outer;
DROP VIEW identifier_nested_inner;
DROP TEMPORARY VARIABLE identifier_nested_col;

-- A temporary SQL function whose body reads a relation via an IDENTIFIER clause, referenced by a
-- temporary view. A SQL function keeps no IDENTIFIER-variable metadata of its own, so the variable
-- is part of the enclosing view's transitive definition: expanding the function while creating the
-- view must record the variable as a referred variable of the view (the function-body analysis
-- context must not discard it). Otherwise re-resolving the stored view text on read fails.
CREATE OR REPLACE TEMPORARY VIEW identifier_fn_target AS SELECT 1 AS c1;
DECLARE OR REPLACE VARIABLE identifier_fn_var STRING DEFAULT 'identifier_fn_target';
CREATE TEMPORARY FUNCTION identifier_fn() RETURN SELECT count(*) FROM IDENTIFIER(identifier_fn_var);
CREATE OR REPLACE TEMPORARY VIEW identifier_fn_view AS SELECT identifier_fn() AS c;
SELECT * FROM identifier_fn_view;
DROP VIEW identifier_fn_view;
DROP TEMPORARY FUNCTION identifier_fn;
DROP VIEW identifier_fn_target;
DROP TEMPORARY VARIABLE identifier_fn_var;

-- ALTER of a LOCAL TEMPORARY view whose new body reads a column via an IDENTIFIER clause. The
-- forwarded referred-variable dependency must be stored so re-analyzing the altered view text on
-- read still resolves the variable.
CREATE OR REPLACE TEMPORARY VIEW identifier_talter_view AS SELECT 1 AS c1;
DECLARE OR REPLACE VARIABLE identifier_talter_col STRING DEFAULT 'c1';
ALTER VIEW identifier_talter_view AS
SELECT IDENTIFIER(identifier_talter_col) AS x FROM VALUES(1) AS t(c1);
SELECT * FROM identifier_talter_view;
DROP VIEW identifier_talter_view;
DROP TEMPORARY VARIABLE identifier_talter_col;

-- CACHE TABLE AS SELECT reads a column via an IDENTIFIER clause. The dependency must be threaded
-- through the cache command so the text-backed temporary view it creates stays resolvable for the
-- eager caching read (and any later lazy read).
DECLARE OR REPLACE VARIABLE identifier_cache_col STRING DEFAULT 'c1';
CACHE TABLE identifier_cache_view AS
SELECT IDENTIFIER(identifier_cache_col) AS x FROM VALUES(1) AS t(c1);
SELECT * FROM identifier_cache_view;
UNCACHE TABLE identifier_cache_view;
DROP TEMPORARY VARIABLE identifier_cache_col;

-- A variable used only to compute the CACHE TABLE target name is not part of the cached view
-- definition, so it must not be recorded as a referred variable of the view.
DECLARE OR REPLACE VARIABLE identifier_cache_name STRING DEFAULT 'identifier_cache_named';
CACHE TABLE IDENTIFIER(identifier_cache_name) AS SELECT 1 AS c1;
SELECT * FROM identifier_cache_named;
UNCACHE TABLE identifier_cache_named;
DROP TEMPORARY VARIABLE identifier_cache_name;
