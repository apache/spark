set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION lpad;
DESCRIBE FUNCTION EXTENDED lpad;

EXPLAIN SELECT
  lpad('hi', 1, '?'),
  lpad('hi', 5, '.'),
  lpad('hi', 6, '123')
FROM src tablesample (1 rows);

SELECT
  lpad('hi', 1, '?'),
  lpad('hi', 5, '.'),
  lpad('hi', 6, '123')
FROM src tablesample (1 rows);
