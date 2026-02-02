-- group by all
-- additional test cases from DuckDB, given to us by Mosha

create temporary view integers as select * from values
  (0, 1),
  (0, 2),
  (1, 3),
  (1, NULL)
  as integers(g, i);


SELECT g, SUM(i) FROM integers GROUP BY ALL ORDER BY 1;

SELECT g, SUM(i), COUNT(*), COUNT(i), SUM(g) FROM integers GROUP BY ALL ORDER BY 1;

SELECT i%2, SUM(i), SUM(g) FROM integers GROUP BY ALL ORDER BY 1;

SELECT (g+i)%2, SUM(i), SUM(g) FROM integers GROUP BY ALL ORDER BY 1;

SELECT (g+i)%2 + SUM(i), SUM(i), SUM(g) FROM integers GROUP BY ALL ORDER BY 1;

SELECT g, i, g%2, SUM(i), SUM(g) FROM integers GROUP BY ALL ORDER BY 1, 2, 3, 4;

SELECT c0 FROM (SELECT 1 c0) t0 GROUP BY ALL HAVING c0>0;

SELECT c0 FROM (SELECT 1 c0, 1 c1 UNION ALL SELECT 1, 2) t0 GROUP BY ALL ORDER BY c0;

SELECT c0 FROM (SELECT 1 c0, 1 c1 UNION ALL SELECT 1, 2) t0 GROUP BY ALL HAVING c1>0 ORDER BY c0;

