set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION abs;
DESCRIBE FUNCTION EXTENDED abs;

EXPLAIN SELECT
  abs(0),
  abs(-1),
  abs(123),
  abs(-9223372036854775807),
  abs(9223372036854775807)
FROM src tablesample (1 rows);

SELECT
  abs(0),
  abs(-1),
  abs(123),
  abs(-9223372036854775807),
  abs(9223372036854775807)
FROM src tablesample (1 rows);

EXPLAIN SELECT
  abs(0.0),
  abs(-3.14159265),
  abs(3.14159265)
FROM src tablesample (1 rows);

SELECT
  abs(0.0),
  abs(-3.14159265),
  abs(3.14159265)
FROM src tablesample (1 rows);
