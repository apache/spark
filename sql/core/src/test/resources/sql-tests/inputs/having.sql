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

-- SPARK-11032: resolve having correctly
SELECT MIN(t.v) FROM (SELECT * FROM hav WHERE v > 0) t HAVING(COUNT(1) > 0);
