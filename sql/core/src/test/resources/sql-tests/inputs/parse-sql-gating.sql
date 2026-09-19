-- parse_sql is off by default while the JSON contract is still evolving.
--SET spark.sql.function.parseSql.enabled=false

SELECT parse_sql('SELECT 1');
