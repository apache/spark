set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION >=;
DESCRIBE FUNCTION EXTENDED >=;

SELECT true>=false, false>=true, false>=false, true>=true FROM src tablesample (1 rows);