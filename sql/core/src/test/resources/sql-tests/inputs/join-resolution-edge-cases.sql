CREATE TABLE t1(col1 INT, col2 STRING);
CREATE TABLE t2(col1 INT, col2 STRING);

-- Join names should be computed as intersection of names of the left and right sides.
SELECT * FROM t2 as t2_1 LEFT JOIN t2 as t2_2 ON t2_1.col1 = t2_2.col1
    NATURAL JOIN t2 as t2_3;
SELECT * FROM t2 as t2_1 RIGHT JOIN t2 as t2_2 ON t2_1.col1 = t2_2.col1
    NATURAL JOIN t2 as t2_3;
SELECT * FROM t2 as t2_1 CROSS JOIN t2 as t2_2 ON t2_1.col1 = t2_2.col1
    NATURAL JOIN t2 as t2_3;
SELECT * FROM t2 as t2_1 INNER JOIN t2 as t2_2 ON t2_1.col1 = t2_2.col1
    NATURAL JOIN t2 as t2_3;

-- Regular join hidden output should be main output ++ old metadata output
SELECT col2 AS alias FROM t1 as t1_1 LEFT ANTI JOIN t1 as t1_2 ON t1_1.col1 = t1_2.col1 ORDER BY col2;
SELECT col2 AS alias FROM t1 as t1_1 LEFT SEMI JOIN t1 as t1_2 ON t1_1.col1 = t1_2.col1 ORDER BY col2;
SELECT col1 FROM t1 as t1_1 LEFT SEMI JOIN t1 as t1_2 ORDER BY col2;
SELECT col1 FROM t1 as t1_1 LEFT SEMI JOIN t1 as t1_2 GROUP BY ALL HAVING MIN(col2) > 1;

-- Retain original join output under Project/Aggregate/Filter
SELECT 1 FROM t2 NATURAL JOIN t1 JOIN t2 ON t2.col1 = t1.col1 WHERE t2.col2 = 1;
SELECT 1 FROM t2 NATURAL JOIN t1 JOIN t2 ON t2.col1 = t1.col1 GROUP BY t2.col2 HAVING t2.col2 = 1;
SELECT 1 FROM t2 NATURAL JOIN t1 JOIN t2 ON t2.col1 = t1.col1 ORDER BY t2.col2;

DROP TABLE t1;
DROP TABLE t2;
