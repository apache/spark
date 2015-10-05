set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION atan;
DESCRIBE FUNCTION EXTENDED atan;

SELECT atan(null)
FROM src tablesample (1 rows);

SELECT atan(1), atan(6), atan(-1.0)
FROM src tablesample (1 rows);
DESCRIBE FUNCTION atan;
DESCRIBE FUNCTION EXTENDED atan;

SELECT atan(null)
FROM src tablesample (1 rows);

SELECT atan(1), atan(6), atan(-1.0)
FROM src tablesample (1 rows);
