set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION space;
DESCRIBE FUNCTION EXTENDED space;

EXPLAIN SELECT
  space(10),
  space(0),
  space(1),
  space(-1),
  space(-100)
FROM src tablesample (1 rows);

SELECT
  length(space(10)),
  length(space(0)),
  length(space(1)),
  length(space(-1)),
  length(space(-100))
FROM src tablesample (1 rows);

SELECT
  space(10),
  space(0),
  space(1),
  space(-1),
  space(-100)
FROM src tablesample (1 rows);

