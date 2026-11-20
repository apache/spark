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

-- An input column takes precedence over a SELECT alias with the same name
SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
FROM VALUES (1, 30, 0), (2, 20, 0), (3, 10, 1) AS t(id, score, rn)
QUALIFY rn = 1
ORDER BY id;

SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
FROM VALUES (1, 30, 0), (2, 20, 0), (3, 10, 1) AS t(id, score, rn)
QUALIFY t.rn = 1
ORDER BY id;

SELECT a, SUM(b) AS b, ROW_NUMBER() OVER (ORDER BY a) AS rn
FROM testData2
GROUP BY a
QUALIFY b > 2
ORDER BY a;

-- A window function of a subquery defines an input column of the outer query
SELECT id, rn, RANK() OVER (ORDER BY id) AS rank
FROM (
  SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
  FROM VALUES (1, 30), (2, 20), (3, 10) AS t(id, score)
) s
QUALIFY rn = 1;
