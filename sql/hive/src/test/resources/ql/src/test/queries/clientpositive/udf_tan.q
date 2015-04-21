set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION tan;
DESCRIBE FUNCTION EXTENDED tan;

SELECT tan(null)
FROM src tablesample (1 rows);

SELECT tan(1), tan(6), tan(-1.0)
FROM src tablesample (1 rows);
DESCRIBE FUNCTION tan;
DESCRIBE FUNCTION EXTENDED tan;

SELECT tan(null)
FROM src tablesample (1 rows);

SELECT tan(1), tan(6), tan(-1.0)
FROM src tablesample (1 rows);
