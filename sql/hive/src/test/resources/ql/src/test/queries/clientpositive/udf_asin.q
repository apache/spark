set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION asin;
DESCRIBE FUNCTION EXTENDED asin;

SELECT asin(null)
FROM src tablesample (1 rows);

SELECT asin(0)
FROM src tablesample (1 rows);

SELECT asin(-0.5), asin(0.66)
FROM src tablesample (1 rows);

SELECT asin(2)
FROM src tablesample (1 rows);
