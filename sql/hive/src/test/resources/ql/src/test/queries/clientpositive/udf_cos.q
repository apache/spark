set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION cos;
DESCRIBE FUNCTION EXTENDED cos;

SELECT cos(null)
FROM src tablesample (1 rows);

SELECT cos(0.98), cos(1.57), cos(-0.5)
FROM src tablesample (1 rows);
