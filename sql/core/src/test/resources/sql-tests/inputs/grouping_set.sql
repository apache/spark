CREATE TEMPORARY VIEW grouping AS SELECT * FROM VALUES
  ("1", "2", "3", 1),
  ("4", "5", "6", 1),
  ("7", "8", "9", 1)
  AS grouping(a, b, c, d);

CREATE TEMPORARY VIEW grouping_null AS SELECT * FROM VALUES
  CAST(NULL AS STRING),
  CAST(NULL AS STRING)
  AS T(e);

-- SPARK-17849: grouping set throws NPE #1
SELECT a, b, c, count(d) FROM grouping GROUP BY a, b, c GROUPING SETS (());

-- SPARK-17849: grouping set throws NPE #2
SELECT a, b, c, count(d) FROM grouping GROUP BY a, b, c GROUPING SETS ((a));

-- SPARK-17849: grouping set throws NPE #3
SELECT a, b, c, count(d) FROM grouping GROUP BY a, b, c GROUPING SETS ((c));

-- SPARK-19509: grouping set should honor input nullability
SELECT COUNT(1) FROM grouping_null GROUP BY e GROUPING SETS (e);

DROP VIEW IF EXISTS grouping;
DROP VIEW IF EXISTS grouping_null;
