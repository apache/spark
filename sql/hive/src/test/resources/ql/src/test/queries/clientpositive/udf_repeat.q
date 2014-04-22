DESCRIBE FUNCTION repeat;
DESCRIBE FUNCTION EXTENDED repeat;

EXPLAIN SELECT
  repeat("Facebook", 3),
  repeat("", 4),
  repeat("asd", 0),
  repeat("asdf", -1)
FROM src LIMIT 1;

SELECT
  repeat("Facebook", 3),
  repeat("", 4),
  repeat("asd", 0),
  repeat("asdf", -1)
FROM src LIMIT 1;
