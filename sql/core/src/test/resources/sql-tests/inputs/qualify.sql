CREATE OR REPLACE TEMPORARY VIEW dealer AS SELECT * FROM VALUES
  (100, 'Fremont', 'Honda Civic', 10),
  (100, 'Fremont', 'Honda Accord', 15),
  (100, 'Fremont', 'Honda CRV', 7),
  (200, 'Dublin', 'Honda Civic', 20),
  (200, 'Dublin', 'Honda Accord', 10),
  (200, 'Dublin', 'Honda CRV', 3),
  (300, 'San Jose', 'Honda Civic', 5),
  (300, 'San Jose', 'Honda Accord', 8)
AS dealer(id, city, car_model, quantity);

SELECT city, car_model, RANK() OVER (PARTITION BY car_model ORDER BY quantity) AS rank
FROM dealer
QUALIFY rank = 1
ORDER BY car_model, city;

SELECT city, car_model
FROM dealer
QUALIFY RANK() OVER (PARTITION BY car_model ORDER BY quantity) = 1
ORDER BY car_model, city;

CREATE OR REPLACE TEMPORARY VIEW testData2 AS SELECT * FROM VALUES
  (1, 1),
  (1, 2),
  (2, 1),
  (2, 2),
  (3, 3)
AS testData2(a, b);

SELECT a, SUM(b) AS total
FROM testData2
GROUP BY a
HAVING SUM(b) > 2
QUALIFY ROW_NUMBER() OVER (ORDER BY a DESC) = 1;

SELECT a, SUM(b) AS total, ROW_NUMBER() OVER (ORDER BY a) AS rn
FROM testData2
GROUP BY a
QUALIFY total > 1
ORDER BY a;

SELECT a, total
FROM (
  SELECT a, SUM(b) AS total
  FROM testData2
  GROUP BY a
) t
QUALIFY ROW_NUMBER() OVER (ORDER BY a) = 1 AND total > 1;

SELECT a
FROM testData2
QUALIFY a = 1;

SELECT a, RANK() OVER (ORDER BY b) AS rank
FROM testData2
QUALIFY COUNT(1) > 1;

-- An input column takes precedence over a same-named window function alias
SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
FROM VALUES (1, 30, 0), (2, 20, 0), (3, 10, 1) AS t(id, score, rn)
QUALIFY rn = 1
ORDER BY id;

SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
FROM VALUES (1, 30, 0), (2, 20, 0), (3, 10, 1) AS t(id, score, rn)
QUALIFY t.rn = 1
ORDER BY id;

-- An aggregate alias still applies when the same-named input column is not grouped
SELECT a, SUM(b) AS b, ROW_NUMBER() OVER (ORDER BY a) AS rn
FROM testData2
GROUP BY a
QUALIFY b > 2
ORDER BY a;

-- ... and over a same-named grouping column
SELECT a AS k, SUM(b) AS a, ROW_NUMBER() OVER (ORDER BY SUM(b)) AS rn
FROM testData2
GROUP BY a
QUALIFY a = 3
ORDER BY k;

-- The input column also takes precedence over a wrapped window function alias, a non-window
-- alias, and the alias of a window function of another window specification
SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) + 0 AS rn
FROM VALUES (1, 30, 0), (2, 20, 0), (3, 10, 1) AS t(id, score, rn)
QUALIFY rn = 1;

SELECT id, score * 10 AS rn, ROW_NUMBER() OVER (ORDER BY id) AS w
FROM VALUES (1, 30, 0), (2, 20, 0), (3, 10, 1) AS t(id, score, rn)
QUALIFY rn = 1;

SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn, SUM(score) OVER () AS total
FROM VALUES (1, 30, 0), (2, 20, 0), (3, 10, 1) AS t(id, score, rn)
QUALIFY rn = 1;

-- A correlated reference in a subquery resolves like the same name outside it
SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
FROM VALUES (1, 30, 0), (2, 20, 0), (3, 10, 1) AS t(id, score, rn)
QUALIFY rn = 1 AND EXISTS (SELECT 1 FROM VALUES (1) AS u(k) WHERE u.k = rn);

SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
FROM VALUES (1, 30), (2, 20), (3, 10) AS t(id, score)
QUALIFY EXISTS (SELECT 1 FROM VALUES (1) AS u(k) WHERE u.k = rn);

SELECT a AS k, ROW_NUMBER() OVER (ORDER BY a) AS a
FROM testData2
GROUP BY a
QUALIFY a = 2 AND EXISTS (SELECT 1 FROM VALUES (2) AS u(x) WHERE u.x = a);

-- With GROUP BY, only a grouping column takes precedence: an ungrouped input column does not
SELECT a, SUM(b) AS total, ROW_NUMBER() OVER (ORDER BY a) AS b
FROM testData2
GROUP BY a
QUALIFY b = 1;

SELECT a, SUM(b) AS total, ROW_NUMBER() OVER (ORDER BY a) AS b
FROM testData2
GROUP BY a
HAVING SUM(b) > 0
QUALIFY b = 1;

SELECT SUM(b) AS total, ROW_NUMBER() OVER (ORDER BY SUM(b)) AS b
FROM testData2
QUALIFY b = 1;

-- A name that only SELECT aliases have resolves as before: to a single alias, even one named
-- like a parameterless function, and ambiguously to two aliases
SELECT id, FIRST_VALUE(name) OVER (ORDER BY id) AS user
FROM VALUES (1, 'alice'), (2, 'bob') AS t(id, name)
QUALIFY user = 'alice'
ORDER BY id;

SELECT id, FIRST_VALUE(name) OVER (ORDER BY id) AS user
FROM VALUES (1, 'alice', 'x'), (2, 'bob', 'y') AS t(id, name, user)
QUALIFY user = 'y';

SELECT score AS rn, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
FROM VALUES (1, 1), (2, 20), (3, 10) AS t(id, score)
QUALIFY rn = 1;

SELECT score AS rn, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
FROM VALUES (1, 30, 0), (2, 20, 0), (3, 10, 1) AS t(id, score, rn)
QUALIFY rn = 1;

SELECT a, SUM(b) AS rn, ROW_NUMBER() OVER (ORDER BY a) AS rn
FROM testData2
GROUP BY a
QUALIFY rn = 1;

-- Two input columns with the name are ambiguous
SELECT t1.id, ROW_NUMBER() OVER (ORDER BY t1.id) AS rn
FROM VALUES (1, 0) AS t1(id, rn) JOIN VALUES (1, 5) AS t2(id, rn) ON t1.id = t2.id
QUALIFY rn = 1;

-- The legacy flag restores the SELECT alias
SET spark.sql.legacy.qualifyPreferSelectAlias=true;
SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
FROM VALUES (1, 30, 0), (2, 20, 0), (3, 10, 1) AS t(id, score, rn)
QUALIFY rn = 1
ORDER BY id;
RESET spark.sql.legacy.qualifyPreferSelectAlias;

-- A window function of a subquery defines an input column of the outer query
SELECT id, rn, RANK() OVER (ORDER BY id) AS rank
FROM (
  SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
  FROM VALUES (1, 30), (2, 20), (3, 10) AS t(id, score)
) s
QUALIFY rn = 1;
