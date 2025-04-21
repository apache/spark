--ONLY_IF spark
set spark.sql.optimizer.supportNestedCorrelatedSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForScalarSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForINSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForEXISTSSubqueries.enabled=true;

-- Spark SQL does not support correlations in ORDER BY clause.
SELECT 1 FROM (SELECT 1) t0(c0) WHERE (SELECT (SELECT 1 ORDER BY c0)) = 1;

-- Spark SQL does not support correlations in LIMIT/OFFSET clause.
SELECT 1 FROM (SELECT 1) t0(c0) WHERE (SELECT (SELECT 1 LIMIT c0)) = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t(ps_supplycost INT, n_name INT);

-- Spark SQL does not support correlated subqueries in FROM clause without
-- explicit Lateral keyword.
SELECT NULL
FROM
    t AS ref_2,
    (SELECT (SELECT NULL
         FROM (FROM t AS ref_5,
              (SELECT ref_2.n_name AS c1))));

-- Spark SQL does not support correlated subqueries in FROM clause without
-- explicit Lateral keyword.
SELECT NULL
FROM
    t AS ref_2,
    (SELECT (SELECT NULL
         FROM (FROM t AS ref_5,
              (SELECT ref_5.ps_supplycost AS c0,
                      ref_2.n_name AS c1))));

DROP TABLE IF EXISTS table_integers;
CREATE TABLE table_integers(i INTEGER);
INSERT INTO table_integers VALUES (1), (2), (3), (NULL);

-- Spark SQL only allow Project/Join/Filter to contain outer references.
-- Any subqueries containing outer references with aggregate expressions must
-- be on the having clause.
SELECT i, (SELECT (SELECT i1.i+SUM(i2.i)) FROM table_integers i2) AS j FROM table_integers i1 ORDER BY i;
SELECT i, (SELECT ((SELECT ((SELECT ((SELECT SUM(i)+SUM(i4.i)+SUM(i3.i)+SUM(i2.i)+SUM(i1.i) FROM table_integers i5)) FROM table_integers i4)) FROM table_integers i3)) FROM table_integers i2) AS j FROM table_integers i1 GROUP BY i ORDER BY i;
SELECT (SELECT (SELECT SUM(i1.i)+SUM(i2.i)+SUM(i3.i) FROM table_integers i3) FROM table_integers i2) FROM table_integers i1 ORDER BY 1;
SELECT i, SUM(i), (SELECT (SELECT SUM(i)+SUM(i1.i)+SUM(i2.i) FROM table_integers) FROM table_integers i2) FROM table_integers i1 GROUP BY i ORDER BY i;

-- ScalarSubquery cannot be in the groupBy/aggregate expressions.
SELECT i, (SELECT SUM(i)+(SELECT 42+i1.i) FROM table_integers) AS j FROM table_integers i1 ORDER BY i;

-- No correlated subqueries in the join condition.
SELECT i, (SELECT SUM(s1.i) FROM table_integers s1 INNER JOIN table_integers s2 ON (SELECT i1.i+s1.i)=(SELECT i1.i+s2.i)) AS j FROM table_integers i1 ORDER BY i;
SELECT i, (SELECT SUM(s1.i) FROM table_integers s1 LEFT OUTER JOIN table_integers s2 ON (SELECT i1.i+s1.i)=(SELECT i1.i+s2.i)) AS j FROM table_integers i1 ORDER BY i

-- Spark sql does not allow mixing outer references and local references in one aggregates.
SELECT (SELECT (SELECT COVAR_POP(i2.i, i3.i) FROM table_integers i3) FROM table_integers i2 ORDER BY i NULLS LAST LIMIT 1) FROM table_integers i1 ORDER BY 1;
SELECT (SELECT (SELECT COVAR_POP(i1.i, i3.i) FROM table_integers i3) FROM table_integers i2 LIMIT 1) FROM table_integers i1 ORDER BY 1;

-- Spark sql does not allow correlations in the right child of left outer join.
SELECT i, (SELECT SUM(ss1.i) FROM (SELECT i FROM table_integers s1 WHERE EXISTS(SELECT i FROM table_integers WHERE i<>s1.i AND s1.i > i)) ss1 LEFT OUTER JOIN (SELECT i FROM table_integers s1 WHERE i=i1.i AND EXISTS(SELECT i FROM table_integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM table_integers i1 ORDER BY i;
SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE EXISTS(SELECT i FROM table_integers WHERE i<>s1.i AND s1.i > i)) ss1 LEFT OUTER JOIN (SELECT i FROM table_integers s1 WHERE i=i1.i AND EXISTS(SELECT i FROM table_integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM table_integers i1 ORDER BY i;
SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE EXISTS(SELECT i FROM table_integers WHERE i<>s1.i AND s1.i > i)) ss1 LEFT OUTER JOIN (SELECT i FROM table_integers s1 WHERE i=i1.i AND EXISTS(SELECT i FROM table_integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM table_integers i1 ORDER BY i;
SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE i=i1.i AND EXISTS(SELECT i FROM table_integers WHERE i<>s1.i AND s1.i>i)) ss1 LEFT OUTER JOIN (SELECT i FROM table_integers s1 WHERE i<>i1.i OR EXISTS(SELECT i FROM table_integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM table_integers i1 ORDER BY i;
SELECT i, (SELECT SUM(s2.i) FROM table_integers s1 LEFT OUTER JOIN (SELECT i FROM table_integers WHERE i=i1.i) s2 ON s1.i=s2.i) AS j FROM table_integers i1 ORDER BY i;
SELECT i, (SELECT SUM(s2.i) FROM table_integers s1 LEFT OUTER JOIN (SELECT i FROM table_integers WHERE i<>i1.i) s2 ON s1.i=s2.i) AS j FROM table_integers i1 ORDER BY i;

DROP TABLE IF EXISTS tbl_ProductSales;
DROP TABLE IF EXISTS another_T;
CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

-- Spark sql does not allow mixing outer references and local references in one aggregates.
SELECT (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t1.col7 <> (SELECT MAX(t1.col1 + t3.col4) FROM another_T t3)) FROM another_T t1;

-- Spark SQL only allow Project/Join/Filter to contain outer references.
-- Any subqueries containing outer references with aggregate expressions must
-- be on the having clause.
SELECT CASE WHEN 1 IN (SELECT (SELECT MAX(col7))) THEN 2 ELSE NULL END FROM another_T t1;
SELECT CASE WHEN 1 IN (SELECT (SELECT MAX(col7)) UNION ALL (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t2.col5 = t2.col1)) THEN 2 ELSE NULL END FROM another_T t1;
SELECT CASE WHEN 1 IN (SELECT (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t2.col5 = t2.col1) UNION ALL (SELECT MAX(col7))) THEN 2 ELSE NULL END FROM another_T t1;
SELECT CASE WHEN NOT col1 NOT IN (SELECT (SELECT MAX(col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON t2.col5 = t1.col1)) THEN 1 ELSE 2 END FROM another_T t1 GROUP BY col1 ORDER BY 1;

DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl(a TINYINT, b SMALLINT, c INTEGER, d BIGINT, e VARCHAR(1), f DATE, g TIMESTAMP);
-- non deterministic
SELECT 1 FROM tbl t1 JOIN tbl t2 ON (t1.d=t2.d) WHERE EXISTS(SELECT t1.c FROM tbl t3 WHERE t1.d+t3.c<100 AND EXISTS(SELECT 1 FROM tbl t4 WHERE t2.f < DATE '2000-01-01'));


