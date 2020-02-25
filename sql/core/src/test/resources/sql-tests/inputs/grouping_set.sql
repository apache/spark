CREATE TEMPORARY VIEW grouping AS SELECT * FROM VALUES
  ("1", "2", "3", 1),
  ("4", "5", "6", 1),
  ("7", "8", "9", 1)
  as grouping(a, b, c, d);

-- SPARK-17849: grouping set throws NPE #1
SELECT a, b, c, count(d) FROM grouping GROUP BY a, b, c GROUPING SETS (());

-- SPARK-17849: grouping set throws NPE #2
SELECT a, b, c, count(d) FROM grouping GROUP BY a, b, c GROUPING SETS ((a));

-- SPARK-17849: grouping set throws NPE #3
SELECT a, b, c, count(d) FROM grouping GROUP BY a, b, c GROUPING SETS ((c));

-- Group sets without explicit group by
SELECT c1, sum(c2) FROM (VALUES ('x', 10, 0), ('y', 20, 0)) AS t (c1, c2, c3) GROUP BY GROUPING SETS (c1);

-- Group sets without group by and with grouping
SELECT c1, sum(c2), grouping(c1) FROM (VALUES ('x', 10, 0), ('y', 20, 0)) AS t (c1, c2, c3) GROUP BY GROUPING SETS (c1);

-- Mutiple grouping within a grouping set
SELECT c1, c2, Sum(c3), grouping__id
FROM   (VALUES ('x', 'a', 10), ('y', 'b', 20) ) AS t (c1, c2, c3)
GROUP  BY GROUPING SETS ( ( c1 ), ( c2 ) )
HAVING GROUPING__ID > 1;

-- Group sets without explicit group by
SELECT grouping(c1) FROM (VALUES ('x', 'a', 10), ('y', 'b', 20)) AS t (c1, c2, c3) GROUP BY GROUPING SETS (c1,c2);

-- Mutiple grouping within a grouping set
SELECT -c1 AS c1 FROM (values (1,2), (3,2)) t(c1, c2) GROUP BY GROUPING SETS ((c1), (c1, c2));

-- complex expression in grouping sets
SELECT a + b, b, sum(c) FROM (VALUES (1,1,1),(2,2,2)) AS t(a,b,c) GROUP BY GROUPING SETS ( (a + b), (b));

-- complex expression in grouping sets
SELECT a + b, b, sum(c) FROM (VALUES (1,1,1),(2,2,2)) AS t(a,b,c) GROUP BY GROUPING SETS ( (a + b), (b + a), (b));

-- more query constructs with grouping sets
SELECT c1 AS col1, c2 AS col2
FROM   (VALUES (1, 2), (3, 2)) t(c1, c2)
GROUP  BY GROUPING SETS ( ( c1 ), ( c1, c2 ) )
HAVING col2 IS NOT NULL
ORDER  BY -col1;

-- negative tests - must have at least one grouping expression
SELECT a, b, c, count(d) FROM grouping GROUP BY WITH ROLLUP;

SELECT a, b, c, count(d) FROM grouping GROUP BY WITH CUBE;

SELECT c1 FROM (values (1,2), (3,2)) t(c1, c2) GROUP BY GROUPING SETS (());

-- duplicate entries in grouping sets
SELECT k1, k2, avg(v) FROM (VALUES (1,1,1),(2,2,2)) AS t(k1,k2,v) GROUP BY GROUPING SETS ((k1),(k1,k2),(k2,k1));

SELECT grouping__id, k1, k2, avg(v) FROM (VALUES (1,1,1),(2,2,2)) AS t(k1,k2,v) GROUP BY GROUPING SETS ((k1),(k1,k2),(k2,k1));

SELECT grouping(k1), k1, k2, avg(v) FROM (VALUES (1,1,1),(2,2,2)) AS t(k1,k2,v) GROUP BY GROUPING SETS ((k1),(k1,k2),(k2,k1));
