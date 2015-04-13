set hive.fetch.task.conversion=more;

EXPLAIN SELECT 3.14, -3.14, 3.14e8, 3.14e-8, -3.14e8, -3.14e-8, 3.14e+8, 3.14E8, 3.14E-8 FROM src LIMIT 1;
SELECT 3.14, -3.14, 3.14e8, 3.14e-8, -3.14e8, -3.14e-8, 3.14e+8, 3.14E8, 3.14E-8 FROM src LIMIT 1;

