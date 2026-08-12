-- End-to-end coverage for parse_command (SPARK-58738).
-- Returns compact JSON for parse-only statement analysis.

-- null input
SELECT parse_command(NULL);

-- basic SELECT classification and references
SELECT parse_command('SELECT a, b FROM t');
SELECT parse_command('SELECT db.my_func(a), count(b) FROM cat.ns.t1 JOIN t2');

-- DML
SELECT parse_command('INSERT INTO t SELECT 1');
SELECT parse_command('DELETE FROM t WHERE a = 1');
SELECT parse_command('UPDATE t SET a = 1 WHERE b = 2');
SELECT parse_command('MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE');

-- DDL / CTAS
SELECT parse_command('CREATE TABLE t (a INT)');
SELECT parse_command('CREATE TABLE t AS SELECT 1 AS a');
SELECT parse_command('DROP TABLE t');

-- Spark-only statements (negative Table 39 codes)
SELECT parse_command('CACHE TABLE t');

-- parameter markers
SELECT parse_command('SELECT * FROM t WHERE a = :foo AND b = ?');

-- CTE: UnresolvedWith CTE bodies are innerChildren
SELECT parse_command('WITH cte AS (SELECT a FROM hidden_base) SELECT a FROM cte');

-- nested subqueries
SELECT parse_command('SELECT (SELECT max(v) FROM scalar_src) AS m, t.a FROM outer_t t WHERE EXISTS (SELECT 1 FROM exists_src e WHERE e.id = t.id)');

-- syntax error: never throws; STANDARD error nested under parse_success=false
SELECT get_json_object(parse_command('SELEC FROM t'), '$.parse_success');
SELECT get_json_object(parse_command('SELEC FROM t'), '$.error.errorClass');
SELECT get_json_object(parse_command('SELEC FROM t'), '$.error.sqlState');

-- batch over a column of SQL text
SELECT sql_text, parse_command(sql_text) FROM VALUES
  ('SELECT 1'),
  ('INSERT INTO t SELECT 1'),
  ('CACHE TABLE t')
AS t(sql_text);

-- BEGIN END scripts contain ';' inside the string literal; use query delimiters
-- so the test harness does not split on those semicolons.
--QUERY-DELIMITER-START
SELECT parse_command('BEGIN SELECT 1; END');
--QUERY-DELIMITER-END

--QUERY-DELIMITER-START
SELECT parse_command('BEGIN SELECT count(a) FROM script_t WHERE c = :p; END');
--QUERY-DELIMITER-END

--QUERY-DELIMITER-START
SELECT parse_command('BEGIN IF (SELECT flag FROM gate) THEN INSERT INTO dest SELECT * FROM src_if; ELSE DELETE FROM src_else; END IF; END');
--QUERY-DELIMITER-END

--QUERY-DELIMITER-START
SELECT parse_command('BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN INSERT INTO err_log SELECT * FROM failing_row; END; SELECT a FROM main_t; END');
--QUERY-DELIMITER-END

-- extract key fields from a script for readable assertions
--QUERY-DELIMITER-START
SELECT
  get_json_object(parse_command('BEGIN SELECT 1; END'), '$.statement_identifier') AS statement_identifier,
  get_json_object(parse_command('BEGIN SELECT 1; END'), '$.statement_code') AS statement_code,
  get_json_object(parse_command('BEGIN SELECT count(a) FROM script_t; END'), '$.table_references') AS table_references,
  get_json_object(parse_command('BEGIN SELECT count(a) FROM script_t; END'), '$.function_references') AS function_references;
--QUERY-DELIMITER-END
