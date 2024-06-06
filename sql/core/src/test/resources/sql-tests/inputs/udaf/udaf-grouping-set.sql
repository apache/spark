CREATE TEMPORARY VIEW grouping AS SELECT * FROM VALUES
  ("1", "2", "3", 1),
  ("4", "5", "6", 1),
  ("7", "8", "9", 1)
  as grouping(a, b, c, d);

-- SPARK-17849: grouping set throws NPE #1
SELECT a, b, c, udaf(d) FROM grouping GROUP BY a, b, c GROUPING SETS (());

-- SPARK-17849: grouping set throws NPE #2
SELECT a, b, c, udaf(d) FROM grouping GROUP BY a, b, c GROUPING SETS ((a));

-- SPARK-17849: grouping set throws NPE #3
SELECT a, b, c, udaf(d) FROM grouping GROUP BY a, b, c GROUPING SETS ((c));

-- Group sets without explicit group by
SELECT c1, udaf(c2) FROM (VALUES ('x', 10, 0), ('y', 20, 0)) AS t (c1, c2, c3) GROUP BY GROUPING SETS (c1);

-- Group sets without group by and with grouping
SELECT c1, udaf(c2), grouping(c1) FROM (VALUES ('x', 10, 0), ('y', 20, 0)) AS t (c1, c2, c3) GROUP BY GROUPING SETS (c1);

-- Mutiple grouping within a grouping set
SELECT c1, c2, udaf(c3), grouping__id
FROM   (VALUES ('x', 'a', 10), ('y', 'b', 20) ) AS t (c1, c2, c3)
GROUP  BY GROUPING SETS ( ( c1 ), ( c2 ) )
HAVING GROUPING__ID > 1;

-- complex expression in grouping sets
SELECT a + b, b, udaf(c) FROM (VALUES (1,1,1),(2,2,2)) AS t(a,b,c) GROUP BY GROUPING SETS ( (a + b), (b));

-- complex expression in grouping sets
SELECT a + b, b, udaf(c) FROM (VALUES (1,1,1),(2,2,2)) AS t(a,b,c) GROUP BY GROUPING SETS ( (a + b), (b + a), (b));

-- negative tests - must have at least one grouping expression
SELECT a, b, c, udaf(d) FROM grouping GROUP BY WITH ROLLUP;

SELECT a, b, c, udaf(d) FROM grouping GROUP BY WITH CUBE;

-- duplicate entries in grouping sets
SELECT k1, k2, udaf(v) FROM (VALUES (1,1,1),(2,2,2)) AS t(k1,k2,v) GROUP BY GROUPING SETS ((k1),(k1,k2),(k2,k1));

SELECT grouping__id, k1, k2, udaf(v) FROM (VALUES (1,1,1),(2,2,2)) AS t(k1,k2,v) GROUP BY GROUPING SETS ((k1),(k1,k2),(k2,k1));

SELECT grouping(k1), k1, k2, udaf(v) FROM (VALUES (1,1,1),(2,2,2)) AS t(k1,k2,v) GROUP BY GROUPING SETS ((k1),(k1,k2),(k2,k1));

-- grouping_id function
SELECT grouping_id(k1, k2), udaf(v) from (VALUES (1,1,1),(2,2,2)) AS t(k1,k2,v) GROUP BY k1, k2 GROUPING SETS ((k2, k1), k1);
