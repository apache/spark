set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION repeat;
DESCRIBE FUNCTION EXTENDED repeat;

EXPLAIN SELECT
  repeat("Facebook", 3),
  repeat("", 4),
  repeat("asd", 0),
  repeat("asdf", -1)
FROM src tablesample (1 rows);

SELECT
  repeat("Facebook", 3),
  repeat("", 4),
  repeat("asd", 0),
  repeat("asdf", -1)
FROM src tablesample (1 rows);
