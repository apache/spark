set hive.fetch.task.conversion=more;

EXPLAIN SELECT 100, 100Y, 100S, 100L FROM src LIMIT 1;

SELECT 100, 100Y, 100S, 100L FROM src LIMIT 1;
