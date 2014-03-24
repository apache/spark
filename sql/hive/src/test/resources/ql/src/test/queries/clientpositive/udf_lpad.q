DESCRIBE FUNCTION lpad;
DESCRIBE FUNCTION EXTENDED lpad;

EXPLAIN SELECT
  lpad('hi', 1, '?'),
  lpad('hi', 5, '.'),
  lpad('hi', 6, '123')
FROM src LIMIT 1;

SELECT
  lpad('hi', 1, '?'),
  lpad('hi', 5, '.'),
  lpad('hi', 6, '123')
FROM src LIMIT 1;
