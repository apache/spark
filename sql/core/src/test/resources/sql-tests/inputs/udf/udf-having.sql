-- This test file was converted from having.sql.

create temporary view hav as select * from values
  ("one", 1),
  ("two", 2),
  ("three", 3),
  ("one", 5)
  as hav(k, v);

-- having clause
SELECT udf(k) AS k, udf(sum(v)) FROM hav GROUP BY k HAVING udf(sum(v)) > 2;

-- having condition contains grouping column
SELECT udf(count(udf(k))) FROM hav GROUP BY v + 1 HAVING v + 1 = udf(2);

-- SPARK-11032: resolve having correctly
SELECT udf(MIN(t.v)) FROM (SELECT * FROM hav WHERE v > 0) t HAVING(udf(COUNT(udf(1))) > 0);

-- SPARK-20329: make sure we handle timezones correctly
SELECT udf(a + b) FROM VALUES (1L, 2), (3L, 4) AS T(a, b) GROUP BY a + b HAVING a + b > udf(1);
