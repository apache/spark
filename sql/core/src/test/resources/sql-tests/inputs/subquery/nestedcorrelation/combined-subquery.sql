--ONLY_IF spark
set spark.sql.optimizer.supportNestedCorrelatedSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForScalarSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForINSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForEXISTSSubqueries.enabled=true;

DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl(a TINYINT, b SMALLINT, c INTEGER, d BIGINT, e VARCHAR(1), f DATE, g TIMESTAMP);

INSERT INTO tbl VALUES (1, 2, 3, 4, '5', DATE '1992-01-01', TIMESTAMP '1992-01-01 00:00:00');

SELECT t1.c+(SELECT t1.b FROM tbl t2 WHERE EXISTS(SELECT t1.b+t2.a)) FROM tbl t1;