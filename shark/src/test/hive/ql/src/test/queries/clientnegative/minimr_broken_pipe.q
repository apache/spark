set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.script.allow.partial.consumption = false;
-- Tests exception in ScriptOperator.close() by passing to the operator a small amount of data
SELECT TRANSFORM(*) USING 'true' AS a, b FROM (SELECT TRANSFORM(*) USING 'echo' AS a, b FROM src LIMIT 1) tmp;