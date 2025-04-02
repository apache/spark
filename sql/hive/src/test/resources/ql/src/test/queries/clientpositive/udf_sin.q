set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION sin;
DESCRIBE FUNCTION EXTENDED sin;

SELECT sin(null)
FROM src tablesample (1 rows);

SELECT sin(0.98), sin(1.57), sin(-0.5)
FROM src tablesample (1 rows);
