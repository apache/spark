set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION named_struct;
DESCRIBE FUNCTION EXTENDED named_struct;

EXPLAIN
SELECT named_struct("foo", 1, "bar", 2),
       named_struct("foo", 1, "bar", 2).foo FROM src tablesample (1 rows);

SELECT named_struct("foo", 1, "bar", 2),
       named_struct("foo", 1, "bar", 2).foo FROM src tablesample (1 rows);
