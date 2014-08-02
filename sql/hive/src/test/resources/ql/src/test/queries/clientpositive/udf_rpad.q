DESCRIBE FUNCTION rpad;
DESCRIBE FUNCTION EXTENDED rpad;

EXPLAIN SELECT
  rpad('hi', 1, '?'),
  rpad('hi', 5, '.'),
  rpad('hi', 6, '123')
FROM src LIMIT 1;

SELECT
  rpad('hi', 1, '?'),
  rpad('hi', 5, '.'),
  rpad('hi', 6, '123')
FROM src LIMIT 1;
