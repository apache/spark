create temporary view hav as select * from values
  ("one", 1),
  ("two", 2),
  ("three", 3),
  ("one", 5)
  as hav(k, v);

-- having clause
SELECT k, sum(v) FROM hav GROUP BY k HAVING sum(v) > 2;

-- having condition contains grouping column
SELECT count(k) FROM hav GROUP BY v + 1 HAVING v + 1 = 2;

-- invalid having condition contains grouping column
SELECT count(k) FROM hav GROUP BY v HAVING v = array(1);

-- SPARK-11032: resolve having correctly
SELECT MIN(t.v) FROM (SELECT * FROM hav WHERE v > 0) t HAVING(COUNT(1) > 0);

-- SPARK-20329: make sure we handle timezones correctly
SELECT a + b FROM VALUES (1L, 2), (3L, 4) AS T(a, b) GROUP BY a + b HAVING a + b > 1;

-- SPARK-31519: Cast in having aggregate expressions returns the wrong result
SELECT SUM(a) AS b, CAST('2020-01-01' AS DATE) AS fake FROM VALUES (1, 10), (2, 20) AS T(a, b) GROUP BY b HAVING b > 10;

-- SPARK-31663: Grouping sets with having clause returns the wrong result
SELECT SUM(a) AS b FROM VALUES (1, 10), (2, 20) AS T(a, b) GROUP BY GROUPING SETS ((b), (a, b)) HAVING b > 10;
SELECT SUM(a) AS b FROM VALUES (1, 10), (2, 20) AS T(a, b) GROUP BY CUBE(a, b) HAVING b > 10;
SELECT SUM(a) AS b FROM VALUES (1, 10), (2, 20) AS T(a, b) GROUP BY ROLLUP(a, b) HAVING b > 10;

-- SPARK-33131: Grouping sets with having clause can not resolve qualified col name.
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY GROUPING SETS(t.c1) HAVING t.c1 = 1;
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY CUBE(t.c1) HAVING t.c1 = 1;
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY ROLLUP(t.c1) HAVING t.c1 = 1;
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY t.c1 HAVING t.c1 = 1;

-- SPARK-28386: Resolve ORDER BY agg function with HAVING clause, while the agg function presents on SELECT list
SELECT k, sum(v) FROM hav GROUP BY k HAVING sum(v) > 2 ORDER BY sum(v);

-- SPARK-28386: Resolve ORDER BY agg function with HAVING clause, while the agg function does not present on SELECT list
SELECT k, sum(v) FROM hav GROUP BY k HAVING sum(v) > 2 ORDER BY avg(v);

-- SPARK-52385: Remove TempResolvedColumns from InheritAnalysisRules name
SELECT sum(v) FROM hav HAVING avg(try_add(v, 1)) = 1;
SELECT sum(v) FROM hav HAVING sum(try_add(v, 1)) = 1;
SELECT sum(v) FROM hav HAVING sum(ifnull(v, 1)) = 1;
SELECT sum(v) FROM hav GROUP BY ALL HAVING sum(ifnull(v, 1)) = 1;
SELECT sum(v) FROM hav GROUP BY v HAVING sum(ifnull(v, 1)) = 1;
SELECT v + 1 FROM hav GROUP BY ALL HAVING avg(try_add(v, 1)) = 1;
SELECT v + 1 FROM hav GROUP BY ALL HAVING avg(try_add(v, 1) + 1) = 1;
SELECT sum(v) FROM hav GROUP BY ifnull(v, 1) + 1 order by ifnull(v, 1) + 1;

-- HAVING condition should prefer table columns over aliases
SELECT 1 AS `2`, 2 FROM VALUES (2) t (`2`) GROUP BY `2` HAVING `2` > 2;
SELECT 2, 1 AS `2` FROM VALUES (2) t (`2`) GROUP BY `2` HAVING `2` > 2;
SELECT 1 AS `2` FROM VALUES (2) t (`2`) GROUP BY `2` HAVING `2` > 2;
SELECT 2 FROM VALUES (2) t (`2`) GROUP BY `2` HAVING `2` > 2;

-- HAVING condition is resolved as a semantically equivalent to one in the SELECT list
SELECT SUM(v) + 1 FROM hav HAVING SUM(v) + 1;
SELECT 1 + SUM(v) FROM hav HAVING SUM(v) + 1;
SELECT SUM(v) + 1 FROM hav HAVING 1 + SUM(v);
SELECT MAX(v) + SUM(v) FROM hav HAVING SUM(v) + MAX(v);
SELECT SUM(v) + 1 + MIN(v) FROM hav HAVING 1 + 1 + 1 + MIN(v) + 1 + SUM(v);

-- HAVING with outer reference to alias in outer project list
SELECT col1 AS alias
FROM values(1)
GROUP BY col1
HAVING (
    SELECT col1 = 1
);

SELECT col1 AS alias
FROM values(named_struct('a', 1))
GROUP BY col1
HAVING (
    SELECT col1.a = 1
);

SELECT col1 AS alias
FROM values(array(1))
GROUP BY col1
HAVING (
    SELECT col1[0] = 1
);

SELECT col1 AS alias
FROM values(map('a', 1))
GROUP BY col1
HAVING (
    SELECT col1[0] = 1
);
