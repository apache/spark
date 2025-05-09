--ONLY_IF spark
set spark.sql.optimizer.supportNestedCorrelatedSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForScalarSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForINSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForEXISTSSubqueries.enabled=true;

DROP TABLE IF EXISTS table_integers;
CREATE TABLE table_integers(i INTEGER);
INSERT INTO table_integers VALUES (1), (2), (3), (NULL);

SELECT
  i,
  (
    SELECT SUM(ss1.i)
    FROM (
      SELECT s1.i
      FROM table_integers s1
      WHERE EXISTS (
        SELECT 1
        FROM table_integers t2
        WHERE s1.i > t2.i
      )
    ) ss1
  ) AS j
FROM table_integers i1
ORDER BY i;

SELECT
  i,
  (
    SELECT SUM(ss2.i)
    FROM (
      SELECT s1.i
      FROM table_integers s1
      WHERE s1.i = i1.i
        AND EXISTS (
          SELECT 1
          FROM table_integers t2
          WHERE t2.i = s1.i
        )
    ) ss2
  ) AS j
FROM table_integers i1
ORDER BY i;

SELECT
  i,
  (
    SELECT SUM(ss1.i) + SUM(ss2.i)
    FROM (
      -- First derived table: values greater than at least one other
      SELECT s1.i
      FROM table_integers s1
      WHERE EXISTS (
        SELECT 1
        FROM table_integers t2
        WHERE s1.i > t2.i
      )
    ) ss1
    LEFT OUTER JOIN (
      -- Second derived table: values equal to at least one other
      SELECT s1.i
      FROM table_integers s1
      WHERE EXISTS (
        SELECT 1
        FROM table_integers t2
        WHERE s1.i = t2.i
      )
    ) ss2
      ON ss1.i = ss2.i
  ) AS j
FROM table_integers i1
ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE CASE WHEN (i=i1.i AND EXISTS (SELECT i FROM table_integers WHERE i=s1.i)) THEN true ELSE false END) ss2) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE i=i1.i AND EXISTS (SELECT i FROM table_integers WHERE i=s1.i)) ss2) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE (SELECT i FROM table_integers WHERE i=s1.i) = 1) ss2) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT i FROM table_integers s1 WHERE i=i1.i AND EXISTS (SELECT i FROM table_integers WHERE i=s1.i)) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE i=i1.i OR i=ANY(SELECT i FROM table_integers WHERE i=s1.i)) ss2) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE CASE WHEN (i=i1.i AND EXISTS(SELECT i FROM table_integers WHERE i=s1.i)) THEN true ELSE false END) ss2) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE i=i1.i AND EXISTS(SELECT i FROM table_integers WHERE i=s1.i)) ss2) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss1.i) FROM (SELECT i FROM table_integers s1 WHERE EXISTS(SELECT i FROM table_integers WHERE i<>s1.i AND s1.i > i)) ss1) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE i=i1.i AND EXISTS(SELECT i FROM table_integers WHERE i<>s1.i AND s1.i>i)) ss1 LEFT OUTER JOIN (SELECT i FROM table_integers s1 WHERE EXISTS(SELECT i FROM table_integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM table_integers i1 ORDER BY i;

DROP TABLE IF EXISTS tbl_ProductSales;
DROP TABLE IF EXISTS another_T;
CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

SELECT (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON EXISTS (SELECT MAX(t1.col1 + t3.col4) AS mymax FROM another_T t3 HAVING t1.col7 <> mymax)) FROM another_T t1;