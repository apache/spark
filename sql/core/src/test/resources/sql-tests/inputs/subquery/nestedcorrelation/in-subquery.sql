--ONLY_IF spark

set spark.sql.optimizer.supportNestedCorrelatedSubqueries.enabled=true;

DROP TABLE IF EXISTS tbl_ProductSales;
DROP TABLE IF EXISTS another_T;
CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

SELECT CASE WHEN 1 IN (SELECT (SELECT MAX(col7))) THEN 2 ELSE NULL END FROM another_T t1;

SELECT CASE WHEN 1 IN (SELECT (SELECT MAX(col7)) UNION ALL (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t2.col5 = t2.col1)) THEN 2 ELSE NULL END FROM another_T t1;

SELECT CASE WHEN 1 IN (SELECT (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t2.col5 = t2.col1) UNION ALL (SELECT MAX(col7))) THEN 2 ELSE NULL END FROM another_T t1;

SELECT (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t1.col7 <> (SELECT MAX(t1.col1 + t3.col4) FROM another_T t3)) FROM another_T t1;

SELECT (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t1.col7 <> ANY(SELECT MAX(t1.col1 + t3.col4) FROM another_T t3)) FROM another_T t1;

SELECT CASE WHEN NOT col1 NOT IN (SELECT (SELECT MAX(col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON t2.col5 = t1.col1)) THEN 1 ELSE 2 END FROM another_T t1 GROUP BY col1 ORDER BY 1;