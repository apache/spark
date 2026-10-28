-- End-to-end coverage for parse_sql ANALYZE mode (SPARK-60593).
--SET spark.sql.function.parseSql.enabled=true

-- default PARSE and explicit PARSE agree
SELECT parse_sql('SELECT 1 AS a');
SELECT parse_sql('SELECT 1 AS a', 'PARSE');

-- ANALYZE types literals without catalog objects
SELECT parse_sql('SELECT 1 AS a', 'ANALYZE');
SELECT get_json_object(parse_sql('SELECT 1 AS a', 'analyze'), '$[0].select_list[0].type.name');

-- missing relation: parse succeeds, analyze reports JSON error
SELECT get_json_object(parse_sql('SELECT a FROM missing_parse_sql_analyze_t', 'ANALYZE'),
  '$[0].parse_success');
SELECT get_json_object(parse_sql('SELECT a FROM missing_parse_sql_analyze_t', 'ANALYZE'),
  '$[0].analyze_success');
SELECT get_json_object(parse_sql('SELECT a FROM missing_parse_sql_analyze_t', 'ANALYZE'),
  '$[0].error.errorClass');
SELECT get_json_object(parse_sql('SELECT a FROM missing_parse_sql_analyze_t', 'ANALYZE'),
  '$[0].source_table_references[0][0]');
SELECT get_json_object(parse_sql('SELECT a FROM missing_parse_sql_analyze_t', 'ANALYZE'),
  '$[0].source_table_references[0][1]');

CREATE OR REPLACE TEMP VIEW parse_sql_analyze_v AS SELECT 1 AS a, 'x' AS b;
SELECT parse_sql('SELECT a, b FROM parse_sql_analyze_v', 'ANALYZE');
DROP VIEW parse_sql_analyze_v;

CREATE TABLE parse_sql_analyze_t (a INT, b STRING) USING parquet;
SELECT parse_sql('SELECT count(a) AS c FROM parse_sql_analyze_t', 'ANALYZE');
DROP TABLE parse_sql_analyze_t;

-- invalid mode fails the query
SELECT parse_sql('SELECT 1', 'bogus');

-- named mode argument
SELECT get_json_object(parse_sql('SELECT 1 AS a', mode => 'ANALYZE'), '$[0].select_list[0].type.name');

-- current_schema override: invoker schema has no table; override does
CREATE DATABASE parse_sql_ctx_a;
CREATE DATABASE parse_sql_ctx_b;
CREATE TABLE parse_sql_ctx_a.t (a INT) USING parquet;
USE parse_sql_ctx_b;
SELECT get_json_object(parse_sql('SELECT a FROM t', 'ANALYZE'), '$[0].analyze_success');
SELECT get_json_object(
  parse_sql('SELECT a FROM t', 'ANALYZE', current_schema => 'parse_sql_ctx_a'),
  '$[0].analyze_success');
SELECT get_json_object(
  parse_sql('SELECT a FROM t', 'ANALYZE', current_schema => 'parse_sql_ctx_a'),
  '$[0].source_table_references[0][1]');
SELECT current_schema();
SELECT get_json_object(
  parse_sql('SELECT 1 AS a', 'ANALYZE', current_schema => '!!!'),
  '$[0].analyze_success');
SELECT get_json_object(
  parse_sql('SELECT 1 AS a', 'ANALYZE', current_schema => '!!!'),
  '$[0].error.errorClass');
USE default;
DROP TABLE parse_sql_ctx_a.t;
DROP DATABASE parse_sql_ctx_a;
DROP DATABASE parse_sql_ctx_b;

-- known session variable not in the persisted schema
SELECT get_json_object(
  parse_sql(
    'SELECT system.session.v AS x',
    'ANALYZE',
    variables => array(named_struct('name', 'system.session.v', 'type', 'INT'))),
  '$[0].select_list[0].type.name');

-- known procedure / SQL function parameter by qualified identity
SELECT get_json_object(
  parse_sql(
    'SELECT proc.parm1 AS x',
    'ANALYZE',
    variables => array(named_struct('name', 'proc.parm1', 'type', 'STRING'))),
  '$[0].select_list[0].type.name');

-- known relation via DDL column list
SELECT get_json_object(
  parse_sql(
    'SELECT a FROM t',
    'ANALYZE',
    relations => array(named_struct('name', 't', 'schema', 'a INT'))),
  '$[0].select_list[0].type.name');

-- invalid variable type stays JSON
SELECT get_json_object(
  parse_sql(
    'SELECT 1 AS a',
    'ANALYZE',
    variables => array(named_struct('name', 'v', 'type', '!!!'))),
  '$[0].analyze_success');
SELECT get_json_object(
  parse_sql(
    'SELECT 1 AS a',
    'ANALYZE',
    variables => array(named_struct('name', 'v', 'type', '!!!'))),
  '$[0].error.errorClass');

-- supplied relation shadows the matching persistent relation
CREATE TABLE parse_sql_overlay_t (a BIGINT) USING parquet;
SELECT get_json_object(
  parse_sql(
    'SELECT a FROM parse_sql_overlay_t',
    'ANALYZE',
    relations => array(named_struct(
      'name', 'parse_sql_overlay_t',
      'schema', 'a STRING'))),
  '$[0].select_list[0].type.name');
DROP TABLE parse_sql_overlay_t;

-- supplied parameter markers type the select list
SELECT get_json_object(
  parse_sql(
    'SELECT abs(?) AS x',
    'ANALYZE',
    parameter_markers => array(named_struct('name', '?', 'type', 'INT'))),
  '$[0].select_list[0].type.name');
SELECT get_json_object(
  parse_sql(
    'SELECT :p AS x',
    'ANALYZE',
    parameter_markers => array(named_struct('name', 'p', 'type', 'STRING'))),
  '$[0].select_list[0].type.name');
