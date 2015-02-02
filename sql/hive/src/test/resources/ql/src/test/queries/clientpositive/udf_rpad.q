set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION rpad;
DESCRIBE FUNCTION EXTENDED rpad;

EXPLAIN SELECT
  rpad('hi', 1, '?'),
  rpad('hi', 5, '.'),
  rpad('hi', 6, '123')
FROM src tablesample (1 rows);

SELECT
  rpad('hi', 1, '?'),
  rpad('hi', 5, '.'),
  rpad('hi', 6, '123')
FROM src tablesample (1 rows);
