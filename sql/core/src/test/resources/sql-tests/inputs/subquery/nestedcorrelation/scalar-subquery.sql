--ONLY_IF spark
set spark.sql.optimizer.supportNestedCorrelatedSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForScalarSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForINSubqueries.enabled=true;
set spark.sql.optimizer.supportNestedCorrelatedSubqueriesForEXISTSSubqueries.enabled=true;

SELECT 1 FROM (SELECT 1) t0(c0) WHERE (SELECT (SELECT c0)) = 1;

DROP TABLE IF EXISTS table_integers;
CREATE TABLE table_integers(i INTEGER);
INSERT INTO table_integers VALUES (1), (2), (3), (NULL);

SELECT i, (SELECT (SELECT 42+i1.i)+42+i1.i) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT (SELECT (SELECT (SELECT 42+i1.i)++i1.i)+42+i1.i)+42+i1.i) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT (SELECT (SELECT (SELECT i1.i+i1.i+i1.i+i1.i+i1.i)))) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT (SELECT (SELECT (SELECT i1.i+i1.i+i1.i+i1.i+i1.i+i2.i) FROM table_integers i2 WHERE i2.i=i1.i))) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(s1.i) FROM (SELECT i FROM table_integers WHERE i=i1.i) s1 LEFT OUTER JOIN table_integers s2 ON s1.i=s2.i) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(s1.i) FROM (SELECT i FROM table_integers WHERE i<>i1.i) s1 LEFT OUTER JOIN table_integers s2 ON s1.i=s2.i) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM table_integers s1 WHERE i=i1.i) ss2) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT * FROM (SELECT (SELECT 42+i1.i)) s1) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT s1.k+s2.k FROM (SELECT (SELECT 42+i1.i) AS k) s1, (SELECT (SELECT 42+i1.i) AS k) s2) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT s1.k+s2.k FROM (SELECT (SELECT 42+i1.i) AS k) s1 LEFT OUTER JOIN (SELECT (SELECT 42+i1.i) AS k) s2 ON s1.k=s2.k) AS j FROM table_integers i1 ORDER BY i;

SELECT i, (SELECT i1.i IN (1, 2, 3, 4, 5, 6, 7, 8)) AS j FROM table_integers i1 ORDER BY i;