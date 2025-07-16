CREATE OR REPLACE TEMPORARY VIEW v1 AS SELECT col1 FROM VALUES
  (42), (17), (99), (5), (42),
  (23), (8), (17), (76), (33),
  (99), (55), (3), (42), (8)
;

CREATE OR REPLACE TEMPORARY VIEW v2 AS SELECT col1 FROM VALUES
  ('apple'), ('banana'), ('cherry'), ('date'), ('apple'),
  ('fig'), ('grape'), ('banana'), ('kiwi'), ('lemon'),
  ('cherry'), ('mango'), ('orange'), ('apple'), ('grape')
;

-- Explicit aliases

-- Baseline

SELECT 2 AS col1 FROM v1 ORDER BY col1;

SELECT 2 AS col1 FROM v1 GROUP BY ALL ORDER BY col1;

SELECT 2 AS col1 FROM v1 GROUP BY ALL HAVING col1 > 50;

SELECT * FROM (
  SELECT col1 AS c, 2 AS col1 FROM v1 ORDER BY col1
) ORDER BY 1;

SELECT * FROM (
  SELECT col1 AS c, 2 AS col1 FROM v1 GROUP BY ALL ORDER BY col1
) ORDER BY 1;

SELECT * FROM (
  SELECT col1 AS c, 2 AS col1 FROM v1 GROUP BY ALL HAVING col1 > 50
) ORDER BY 1;

-- Conflict in main output

SELECT 2 AS col1, 3 AS col1 FROM v1 ORDER BY col1;

SELECT 2 AS col1, 3 AS col1 FROM v1 GROUP BY ALL ORDER BY col1;

SELECT 2 AS col1, 3 AS col1 FROM v1 GROUP BY ALL HAVING col1 > 50;

SELECT col1 AS c, 2 AS col1, 3 AS col1 FROM v1 ORDER BY col1;

SELECT col1 AS c, 2 AS col1, 3 AS col1 FROM v1 GROUP BY ALL ORDER BY col1;

SELECT * FROM (
  SELECT col1 AS c, 2 AS col1, 3 AS col1 FROM v1 GROUP BY ALL HAVING col1 > 50
) ORDER BY 1;

SELECT col1, 2 AS col1 FROM v1 ORDER BY col1;

SELECT col1, 2 AS col1 FROM v1 GROUP BY ALL ORDER BY col1;

SELECT * FROM (
  SELECT col1, 2 AS col1 FROM v1 GROUP BY ALL HAVING col1 > 50
) ORDER BY 1;

-- Conflict in hidden output

SELECT 3 AS col1 FROM (SELECT 1 AS col1, 2 AS col1) ORDER BY col1;

SELECT 3 AS col1 FROM (SELECT 1 AS col1, 2 AS col1) GROUP BY ALL ORDER BY col1;

SELECT 3 AS col1 FROM (SELECT 1 AS col1, 2 AS col1) GROUP BY ALL HAVING col1 > 50;

SELECT col1 AS c, 3 AS col1 FROM (SELECT 1 AS col1, 2 AS col1) ORDER BY col1;

SELECT col1 AS c, 3 AS col1 FROM (SELECT 1 AS col1, 2 AS col1) GROUP BY ALL ORDER BY col1;

SELECT col1 AS c, 3 AS col1 FROM (SELECT 1 AS col1, 2 AS col1) GROUP BY ALL HAVING col1 > 50;

-- Implicit aliases

-- Baseline

SELECT 'col1' FROM v2 ORDER BY col1;

SELECT 'col1' FROM v2 GROUP BY ALL ORDER BY col1;

SELECT 'col1' FROM v2 GROUP BY ALL HAVING col1 > 'banana';

SELECT * FROM (
  SELECT col1 AS c, 'col1' FROM v2 ORDER BY col1
) ORDER BY 1;

SELECT * FROM (
  SELECT col1 AS c, 'col1' FROM v2 GROUP BY ALL ORDER BY col1
) ORDER BY 1;

SELECT * FROM (
  SELECT col1 AS c, 'col1' FROM v2 GROUP BY ALL HAVING col1 > 'banana'
) ORDER BY 1;

-- Conflict in main output

SELECT 'col1', 'col1' FROM v2 ORDER BY col1;

SELECT 'col1', 'col1' FROM v2 GROUP BY ALL ORDER BY col1;

SELECT 'col1', 'col1' FROM v2 GROUP BY ALL HAVING col1 > 'banana';

SELECT col1 AS c, 'col1', 'col1' FROM v2 ORDER BY col1;

SELECT col1 AS c, 'col1', 'col1' FROM v2 GROUP BY ALL ORDER BY col1;

SELECT * FROM (
  SELECT col1 AS c, 'col1', 'col1' FROM v2 GROUP BY ALL HAVING col1 > 'banana'
) ORDER BY 1;

SELECT col1, 'col1' FROM v2 ORDER BY col1;

SELECT col1, 'col1' FROM v2 GROUP BY ALL ORDER BY col1;

SELECT * FROM (
  SELECT col1, 'col1' FROM v2 GROUP BY ALL HAVING col1 > 'banana'
) ORDER BY 1;

-- Conflict in hidden output

SELECT 'col1' FROM (SELECT 'a' AS col1, 'b' AS col1) ORDER BY col1;

SELECT 'col1' FROM (SELECT 'a' AS col1, 'b' AS col1) GROUP BY ALL ORDER BY col1;

SELECT 'col1' FROM (SELECT 'a' AS col1, 'b' AS col1) GROUP BY ALL HAVING col1 > 'banana';

SELECT col1 AS c, 'col1' FROM (SELECT 'a' AS col1, 'b' AS col1) ORDER BY col1;

SELECT col1 AS c, 'col1' FROM (SELECT 'a' AS col1, 'b' AS col1) GROUP BY ALL ORDER BY col1;

SELECT col1 AS c, 'col1' FROM (SELECT 'a' AS col1, 'b' AS col1) GROUP BY ALL HAVING col1 > 'banana';

DROP VIEW v2;
DROP VIEW v1;
