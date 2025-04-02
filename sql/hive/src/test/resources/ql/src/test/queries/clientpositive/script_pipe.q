set hive.exec.script.allow.partial.consumption = true;
-- Tests exception in ScriptOperator.close() by passing to the operator a small amount of data
EXPLAIN SELECT TRANSFORM(*) USING 'true' AS a, b, c FROM (SELECT * FROM src LIMIT 1) tmp;
-- Tests exception in ScriptOperator.processOp() by passing extra data needed to fill pipe buffer
EXPLAIN SELECT TRANSFORM(key, value, key, value, key, value, key, value, key, value, key, value) USING 'head -n 1' as a,b,c,d FROM src;

SELECT TRANSFORM(*) USING 'true' AS a, b, c FROM (SELECT * FROM src LIMIT 1) tmp;
SELECT TRANSFORM(key, value, key, value, key, value, key, value, key, value, key, value) USING 'head -n 1' as a,b,c,d FROM src;
