CREATE OR REPLACE TEMPORARY VIEW v1 AS SELECT * FROM VALUES
  (1, 2),
  (3, 5),
  (7, 2),
  (1, 2),
  (8, 3),
  (4, 9),
  (5, 7),
  (9, 4),
  (2, 6),
  (5, 7),
  (3, 8),
  (6, 1),
  (4, 9),
  (7, 5),
  (2, 8)
;
CREATE OR REPLACE TEMPORARY VIEW v2 AS SELECT * FROM VALUES
  (1, 2),
  (5, 9),
  (8, 4),
  (7, 5),
  (4, 9),
  (3, 1),
  (7, 2),
  (6, 8),
  (9, 7),
  (2, 3),
  (8, 3),
  (5, 7),
  (1, 6),
  (3, 2),
  (9, 8)
;

-- ORDER BY

-- Referencing a grouping attribute

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 ORDER BY col1;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 ORDER BY col2;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY v1.col1;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY v1.col2;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY v2.col1;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY v2.col2;

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 ORDER BY 1 + col1, 2 + col2;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 ORDER BY 1 + col2, 2 + col1;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 ORDER BY 1 + v1.col1, 2 + v1.col2;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 ORDER BY 1 + v1.col2, 2 + v1.col1;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 ORDER BY 1 + v1.col1, 2 + v2.col2;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 ORDER BY 1 + v2.col1, 2 + v1.col2;

-- Referencing any attribute under aggregate expression

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 ORDER BY MAX(col1);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 ORDER BY MAX(col2);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY MAX(v1.col1);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY MAX(v1.col2);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY MAX(v2.col1);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY MAX(v2.col2);

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 ORDER BY 1 + MAX(col1), 2 + MIN(col2);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 ORDER BY 1 + MAX(col2), 2 + MIN(col1);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 ORDER BY 1 + MAX(v1.col1), 2 + MIN(v1.col2);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 ORDER BY 1 + MAX(v1.col2), 2 + MIN(v1.col1);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 ORDER BY 1 + MAX(v1.col1), 2 + MIN(v2.col2);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 ORDER BY 1 + MAX(v2.col1), 2 + MIN(v1.col2);

-- ORDER BY with LCA below

-- Referencing a grouping attribute

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 ORDER BY col1;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 ORDER BY col2;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY v1.col1;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY v1.col2;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY v2.col1;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY v2.col2;

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 ORDER BY 1 + col1, 2 + col2;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 ORDER BY 1 + col2, 2 + col1;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 ORDER BY 1 + v1.col1, 2 + v1.col2;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 ORDER BY 1 + v1.col2, 2 + v1.col1;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 ORDER BY 1 + v1.col1, 2 + v2.col2;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 ORDER BY 1 + v2.col1, 2 + v1.col2;

-- Referencing any attribute under aggregate expression

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 ORDER BY MAX(col1);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 ORDER BY MAX(col2);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY MAX(v1.col1);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY MAX(v1.col2);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY MAX(v2.col1);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 ORDER BY MAX(v2.col2);

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 ORDER BY 1 + MAX(col1), 2 + MIN(col2);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 ORDER BY 1 + MAX(col2), 2 + MIN(col1);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 ORDER BY 1 + MAX(v1.col1), 2 + MIN(v1.col2);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 ORDER BY 1 + MAX(v1.col2), 2 + MIN(v1.col1);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 ORDER BY 1 + MAX(v1.col1), 2 + MIN(v2.col2);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 ORDER BY 1 + MAX(v2.col1), 2 + MIN(v1.col2);

-- HAVING

-- Referencing a grouping attribute

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING col1 > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING col2 > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v1.col1 > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v1.col2 > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v2.col1 > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v2.col2 > 5;

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + col1 > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + col2 > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + v1.col1 > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + v1.col2 > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + v1.col1 > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + v2.col1 > 5;

-- Referencing any attribute under aggregate expression

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING MAX(col1) > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING MAX(col2) > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v1.col1) > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v1.col2) > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v2.col1) > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v2.col2) > 5;

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + MAX(col1) > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + MAX(col2) > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + MAX(v1.col1) > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + MAX(v1.col2) > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + MAX(v1.col1) > 5;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + MAX(v2.col1) > 5;

-- HAVING with LCA below

-- Referencing a grouping attribute

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING col1 > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING col2 > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v1.col1 > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v1.col2 > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v2.col1 > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v2.col2 > 5;

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + col1 > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + col2 > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + v1.col1 > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + v1.col2 > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + v1.col1 > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + v2.col1 > 5;

-- Referencing any attribute under aggregate expression

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING MAX(col1) > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING MAX(col2) > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v1.col1) > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v1.col2) > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v2.col1) > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v2.col2) > 5;

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + MAX(col1) > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + MAX(col2) > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + MAX(v1.col1) > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + MAX(v1.col2) > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + MAX(v1.col1) > 5;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + MAX(v2.col1) > 5;

-- ORDER BY and HAVING

-- Referencing a grouping attribute

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING col1 > 5 ORDER BY col1;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING col2 > 5 ORDER BY col2;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v1.col1 > 5 ORDER BY v1.col1;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v1.col2 > 5 ORDER BY v1.col2;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v2.col1 > 5 ORDER BY v2.col1;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v2.col2 > 5 ORDER BY v2.col2;

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + col1 > 5 ORDER BY 1 + col1, 2 + col2;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + col2 > 5 ORDER BY 1 + col2, 2 + col1;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + v1.col1 > 5 ORDER BY 1 + v1.col1, 2 + v1.col2;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + v1.col2 > 5 ORDER BY 1 + v1.col2, 2 + v1.col1;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + v1.col1 > 5 ORDER BY 1 + v1.col1, 2 + v2.col2;
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + v2.col1 > 5 ORDER BY 1 + v2.col1, 2 + v1.col2;

-- Referencing any attribute under aggregate expression.
-- These mostly fail because Spark does not support referencing any attributes under aggregate expressions in ORDER BY on top of HAVING

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING MAX(col1) > 5 ORDER BY MAX(col1);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING MAX(col2) > 5 ORDER BY MAX(col2);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v1.col1) > 5 ORDER BY MAX(v1.col1);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v1.col2) > 5 ORDER BY MAX(v1.col2);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v2.col1) > 5 ORDER BY MAX(v2.col1);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v2.col2) > 5 ORDER BY MAX(v2.col2);

SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + MAX(col1) > 5 ORDER BY 1 + MAX(col1), 2 + MIN(col2);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + MAX(col2) > 5 ORDER BY 1 + MAX(col2), 2 + MIN(col1);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + MAX(v1.col1) > 5 ORDER BY 1 + MAX(v1.col1), 2 + MIN(v1.col2);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + MAX(v1.col2) > 5 ORDER BY 1 + MAX(v1.col2), 2 + MIN(v1.col1);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + MAX(v1.col1) > 5 ORDER BY 1 + MAX(v1.col1), 2 + MIN(v2.col2);
SELECT SUM(col2) FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + MAX(v1.col1) > 5 ORDER BY 1 + MAX(v1.col1), 2 + MIN(v2.col2);

-- ORDER BY and HAVING with LCA below

-- Referencing a grouping attribute

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING col1 > 5 ORDER BY col1;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING col2 > 5 ORDER BY col2;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v1.col1 > 5 ORDER BY v1.col1;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v1.col2 > 5 ORDER BY v1.col2;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v2.col1 > 5 ORDER BY v2.col1;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING v2.col2 > 5 ORDER BY v2.col2;

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + col1 > 5 ORDER BY 1 + col1, 2 + col2;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + col2 > 5 ORDER BY 1 + col2, 2 + col1;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + v1.col1 > 5 ORDER BY 1 + v1.col1, 2 + v1.col2;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + v1.col2 > 5 ORDER BY 1 + v1.col2, 2 + v1.col1;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + v1.col1 > 5 ORDER BY 1 + v1.col1, 2 + v2.col2;
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + v2.col1 > 5 ORDER BY 1 + v2.col1, 2 + v1.col2;

-- Referencing any attribute under aggregate expression.
-- These mostly fail because Spark does not support referencing any attributes under aggregate expressions in ORDER BY on top of HAVING

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING MAX(col1) > 5 ORDER BY MAX(col1);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 HAVING MAX(col2) > 5 ORDER BY MAX(col2);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v1.col1) > 5 ORDER BY MAX(v1.col1);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v1.col2) > 5 ORDER BY MAX(v1.col2);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v2.col1) > 5 ORDER BY MAX(v2.col1);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 HAVING MAX(v2.col2) > 5 ORDER BY MAX(v2.col2);

SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + MAX(col1) > 5 ORDER BY 1 + MAX(col1), 2 + MIN(col2);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY col1 + 1, col2 + 2 HAVING 1 + MAX(col2) > 5 ORDER BY 1 + MAX(col2), 2 + MIN(col1);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + MAX(v1.col1) > 5 ORDER BY 1 + MAX(v1.col1), 2 + MIN(v1.col2);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v1.col2 + 2 HAVING 1 + MAX(v1.col2) > 5 ORDER BY 1 + MAX(v1.col2), 2 + MIN(v1.col1);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + MAX(v1.col1) > 5 ORDER BY 1 + MAX(v1.col1), 2 + MIN(v2.col2);
SELECT SUM(col2) AS a, a + 1 AS b, b + 1 AS c FROM v1 NATURAL JOIN v2 GROUP BY v1.col1 + 1, v2.col2 + 2 HAVING 1 + MAX(v1.col1) > 5 ORDER BY 1 + MAX(v1.col1), 2 + MIN(v2.col2);

-- Clean up
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS v2;
