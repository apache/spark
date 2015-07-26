set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION acos;
DESCRIBE FUNCTION EXTENDED acos;

SELECT acos(null)
FROM src tablesample (1 rows);

SELECT acos(0)
FROM src tablesample (1 rows);

SELECT acos(-0.5), asin(0.66)
FROM src tablesample (1 rows);

SELECT acos(2)
FROM src tablesample (1 rows);
