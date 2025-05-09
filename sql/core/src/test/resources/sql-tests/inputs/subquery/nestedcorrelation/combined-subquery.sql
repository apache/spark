--ONLY_IF spark
set spark.sql.optimizer.supportNestedCorrelatedSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForScalarSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForINSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForEXISTSSubqueries.enabled=true;

DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl(a TINYINT, b SMALLINT, c INTEGER, d BIGINT, e VARCHAR(1), f DATE, g TIMESTAMP);

INSERT INTO tbl VALUES (1, 2, 3, 4, '5', DATE '1992-01-01', TIMESTAMP '1992-01-01 00:00:00');

SELECT t1.c+(SELECT t1.b FROM tbl t2 WHERE EXISTS(SELECT t1.b+t2.a)) FROM tbl t1;

-- non deterministic due to the timestamp type, the query itself is supported.
SELECT 1 FROM tbl t1 JOIN tbl t2 ON (t1.d=t2.d) WHERE EXISTS(SELECT t1.c FROM tbl t3 WHERE t1.d+t3.c<100 AND EXISTS(SELECT 1 FROM tbl t4 WHERE t2.f < DATE '2000-01-01'));