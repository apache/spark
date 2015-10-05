set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION bin;
DESCRIBE FUNCTION EXTENDED bin;

SELECT
  bin(1),
  bin(0),
  bin(99992421)
FROM src tablesample (1 rows);

-- Negative numbers should be treated as two's complement (64 bit).
SELECT bin(-5) FROM src tablesample (1 rows);
