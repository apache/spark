set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION div;
DESCRIBE FUNCTION EXTENDED div;

SELECT 3 DIV 2 FROM SRC tablesample (1 rows);
