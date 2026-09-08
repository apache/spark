-- End-to-end coverage for parse_sql (SPARK-58738).
-- Returns compact JSON for parse-only statement analysis via SparkSqlParser.
-- Off by default while the JSON contract is still evolving.
--SET spark.sql.function.parseSql.enabled=true

-- null input
SELECT parse_sql(NULL);

-- basic SELECT classification and references
SELECT parse_sql('SELECT a, b FROM t');
SELECT parse_sql('SELECT db.my_func(a), count(b) FROM cat.ns.t1 JOIN t2');

-- JSON-path access over one shared successful parse result
SELECT
  get_json_object(result, '$.statement_identifier') AS statement_identifier,
  get_json_object(result, '$.source_table_references[0][0]') AS first_table,
  get_json_object(result, '$.select_list[1].name[0]') AS second_column
FROM (SELECT parse_sql('SELECT a, b FROM t') AS result);

-- DML
SELECT parse_sql('INSERT INTO t SELECT 1');
SELECT parse_sql('DELETE FROM t WHERE a = 1');
SELECT parse_sql('UPDATE t SET a = 1 WHERE b = 2');
SELECT parse_sql('MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE');

-- DDL / CTAS
SELECT parse_sql('CREATE TABLE t (a INT)');
SELECT parse_sql('CREATE TABLE t AS SELECT 1 AS a');
SELECT parse_sql('DROP TABLE t');

-- Spark-only statements (negative Table 39 codes)
SELECT parse_sql('CACHE TABLE t');

-- TABLE / VALUES are SELECT-shaped (not Unrecognized)
SELECT parse_sql('TABLE t');
SELECT parse_sql('VALUES (1), (2)');

-- function / variable names are not target or source table references
SELECT parse_sql('CREATE FUNCTION f AS ''x'' USING JAR ''y.jar''');
SELECT parse_sql('DECLARE VARIABLE x INT');

-- parameter markers
SELECT parse_sql('SELECT * FROM t WHERE a = :foo AND b = ?');

-- CTE: lineage excludes CTE names; still walks CTE bodies for real tables
SELECT parse_sql('WITH cte AS (SELECT a FROM hidden_base) SELECT a FROM cte');

-- CTE shadowing is scoped: the inner CTE real_t does not hide the outer table
SELECT parse_sql('SELECT * FROM real_t WHERE EXISTS (WITH real_t AS (SELECT * FROM inner_base) SELECT * FROM real_t)');

-- a CTE definition sees only preceding aliases, so b below is the real table
SELECT parse_sql('WITH a AS (SELECT * FROM b), b AS (SELECT 1 AS x) SELECT * FROM a');

-- nested subqueries
SELECT parse_sql('SELECT (SELECT max(v) FROM scalar_src) AS m, t.a FROM outer_t t WHERE EXISTS (SELECT 1 FROM exists_src e WHERE e.id = t.id)');

-- functions in projection, window, join, TVF, predicates, subquery, grouping, and ordering
SELECT parse_sql(
'SELECT coalesce(t.a, 0), sum(abs(t.b)) OVER (
   PARTITION BY lower(t.c) ORDER BY length(t.d))
 FROM left_t t
 JOIN right_t r ON hash(t.id) = hash(r.id)
 JOIN LATERAL range(cast(t.n AS BIGINT)) rng
 WHERE startswith(t.c, ''x'')
   AND EXISTS (SELECT max(s.v) FROM scalar_t s WHERE s.id = t.id)
 GROUP BY coalesce(t.a, 0), t.b, t.c, t.d
 HAVING count_if(t.b > 0) > 0
 ORDER BY greatest(t.a, 1)');

-- functions and tables throughout a multiline MERGE
SELECT parse_sql(
'MERGE INTO target t
 USING (
   SELECT id, normalize_name(name) AS name
   FROM source
   WHERE is_valid(id)
 ) s
 ON hash(t.id) = hash(s.id)
 WHEN MATCHED AND should_update(t.name, s.name) THEN
   UPDATE SET name = coalesce(s.name, upper(t.name))
 WHEN NOT MATCHED THEN
   INSERT (id, name) VALUES (s.id, lower(s.name))');

-- functions embedded in DDL column defaults
SELECT parse_sql(
'CREATE TABLE defaults (
   created DATE DEFAULT current_date(),
   normalized STRING DEFAULT upper(''x'')
 )');

-- syntax error: dump the complete STANDARD error, including query context
SELECT parse_sql('SELEC FROM t');

-- JSON-path access over one shared parse result
SELECT
  get_json_object(result, '$.parse_success') AS parse_success,
  get_json_object(result, '$.error.errorClass') AS error_class,
  get_json_object(result, '$.error.queryContext[0].fragment') AS fragment
FROM (SELECT parse_sql('SELEC FROM t') AS result);

-- full multiline parse-time validation error, including context and location
SELECT parse_sql(
'SELECT *
 FROM t
 ORDER BY a
 CLUSTER BY b');

-- JSON-path access over one shared multiline parse result
SELECT
  get_json_object(result, '$.error.errorClass') AS error_class,
  get_json_object(result, '$.error.line') AS line,
  get_json_object(result, '$.error.position') AS position,
  get_json_object(result, '$.error.queryContext[0].startIndex') AS start_index
FROM (
  SELECT parse_sql(
'SELECT *
 FROM t
 ORDER BY a
 CLUSTER BY b') AS result
);

-- parse-only validation errors beyond PARSE_SYNTAX_ERROR
SELECT parse_sql('');
SELECT parse_sql('USE bad-name');
SELECT parse_sql('WITH c AS (SELECT 1), c AS (SELECT 2) SELECT * FROM c');
SELECT parse_sql('MERGE INTO target USING source ON target.id = source.id');
SELECT parse_sql('EXPLAIN SELECT 1');
SELECT parse_sql('SET spark.sql.adaptive.enabled=true');
SELECT parse_sql('ADD JAR /tmp/x.jar');
SELECT parse_sql('CREATE VIEW v AS SELECT a, b FROM t');
SELECT parse_sql('SELECT 1 AS IDENTIFIER(''alias.field'')');
SELECT parse_sql('SELECT DATE ''not-a-date''');

-- location for an error inside a multiline script
--QUERY-DELIMITER-START
SELECT parse_sql(
'BEGIN
   SELECT 1;
   SELEC 2;
 END');
--QUERY-DELIMITER-END

-- JSON-path access over one shared scripting parse result
--QUERY-DELIMITER-START
SELECT
  get_json_object(result, '$.error.errorClass') AS error_class,
  get_json_object(result, '$.error.line') AS line,
  get_json_object(result, '$.error.position') AS position,
  get_json_object(result, '$.error.queryContext[0].fragment') AS fragment
FROM (
  SELECT parse_sql(
'BEGIN
   SELECT 1;
   SELEC 2;
 END') AS result
);
--QUERY-DELIMITER-END

-- location for a SQL scripting semantic validation error
--QUERY-DELIMITER-START
SELECT parse_sql(
'BEGIN
   lbl_begin: BEGIN
     SELECT 1;
   END lbl_end;
 END');
--QUERY-DELIMITER-END

-- JSON-path access over one shared scripting validation result
--QUERY-DELIMITER-START
SELECT
  get_json_object(result, '$.error.errorClass') AS error_class,
  get_json_object(result, '$.error.line') AS line,
  get_json_object(result, '$.error.position') AS position,
  get_json_object(result, '$.error.queryContext[0].fragment') AS fragment
FROM (
  SELECT parse_sql(
'BEGIN
   lbl_begin: BEGIN
     SELECT 1;
   END lbl_end;
 END') AS result
);
--QUERY-DELIMITER-END

-- batch over a column of SQL text
SELECT sql_text, parse_sql(sql_text) FROM VALUES
  ('SELECT 1'),
  ('INSERT INTO t SELECT 1'),
  ('CACHE TABLE t')
AS t(sql_text);

-- BEGIN END scripts contain ';' inside the string literal; use query delimiters
-- so the test harness does not split on those semicolons.
--QUERY-DELIMITER-START
SELECT parse_sql('BEGIN SELECT 1; END');
--QUERY-DELIMITER-END

--QUERY-DELIMITER-START
SELECT parse_sql('BEGIN SELECT count(a) FROM script_t WHERE c = :p; END');
--QUERY-DELIMITER-END

-- Positional markers under SingleStatement must not be double-counted.
--QUERY-DELIMITER-START
SELECT parse_sql('BEGIN SELECT * FROM t WHERE a = ?; END');
--QUERY-DELIMITER-END

--QUERY-DELIMITER-START
SELECT parse_sql('BEGIN IF (SELECT flag FROM gate) THEN INSERT INTO dest SELECT * FROM src_if; ELSE DELETE FROM src_else; END IF; END');
--QUERY-DELIMITER-END

--QUERY-DELIMITER-START
SELECT parse_sql('BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN INSERT INTO err_log SELECT * FROM failing_row; END; SELECT a FROM main_t; END');
--QUERY-DELIMITER-END

-- Complex, genuinely multiline script: dump the complete JSON result.
--QUERY-DELIMITER-START
SELECT parse_sql(
'BEGIN
   DECLARE EXIT HANDLER FOR SQLEXCEPTION
   BEGIN
     INSERT INTO error_log
     SELECT format_string(''%s'', message) FROM error_source;
   END;

   WITH prepared AS (
     SELECT id, normalize_name(name) AS name
     FROM input_names
     WHERE is_valid(id)
   )
   INSERT INTO output_names
   SELECT id, upper(name) FROM prepared;

   IF EXISTS (SELECT 1 FROM control_flags WHERE enabled()) THEN
     UPDATE update_target
     SET value = coalesce((SELECT max(value) FROM update_source), 0)
     WHERE should_update(id);
   ELSE
     DELETE FROM delete_target
     WHERE id IN (SELECT id FROM delete_source WHERE expired(ts));
   END IF;

   FOR row AS
     SELECT id FROM loop_source WHERE ready(id)
   DO
     SELECT audit(row.id), count(*) FROM loop_body;
   END FOR;
 END');
--QUERY-DELIMITER-END
