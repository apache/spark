--ONLY_IF spark
set spark.sql.optimizer.supportNestedCorrelatedSubqueries.enabled=true;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0(c0 INT);

SELECT 1 FROM (SELECT 1) t0(c0) WHERE (SELECT (SELECT c0));

SELECT 1 FROM (SELECT 1) t0(c0) WHERE (SELECT (SELECT 1 ORDER BY c0));

SELECT 1 FROM (SELECT 1) t0(c0) WHERE (SELECT (SELECT 1 LIMIT c0));

DROP TABLE IF EXISTS t;
CREATE TABLE t(ps_supplycost INT, n_name INT);

SELECT NULL
FROM
    t AS ref_2,
    (SELECT (SELECT NULL
         FROM (FROM t AS ref_5,
              (SELECT ref_2.n_name AS c1))));

SELECT NULL
FROM
    t AS ref_2,
    (SELECT (SELECT NULL
         FROM (FROM t AS ref_5,
              (SELECT ref_5.ps_supplycost AS c0,
                      ref_2.n_name AS c1))));

DROP TABLE IF EXISTS integers;
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);

SELECT i, (SELECT (SELECT 42+i1.i)+42+i1.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT (SELECT (SELECT (SELECT 42+i1.i)++i1.i)+42+i1.i)+42+i1.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT (SELECT i1.i+SUM(i2.i)) FROM integers i2) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT (SELECT (SELECT (SELECT i1.i+i1.i+i1.i+i1.i+i1.i)))) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(i)+(SELECT 42+i1.i) FROM integers) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT ((SELECT ((SELECT ((SELECT SUM(i)+SUM(i4.i)+SUM(i3.i)+SUM(i2.i)+SUM(i1.i) FROM integers i5)) FROM integers i4)) FROM integers i3)) FROM integers i2) AS j FROM integers i1 GROUP BY i ORDER BY i;

SELECT i, (SELECT (SELECT (SELECT (SELECT i1.i+i1.i+i1.i+i1.i+i1.i+i2.i) FROM integers i2 WHERE i2.i=i1.i))) AS j FROM integers i1 ORDER BY i;

SELECT (SELECT (SELECT SUM(i1.i)+SUM(i2.i)+SUM(i3.i) FROM integers i3) FROM integers i2) FROM integers i1 ORDER BY 1;

SELECT i, (SELECT SUM(s1.i) FROM integers s1 INNER JOIN integers s2 ON (SELECT i1.i+s1.i)=(SELECT i1.i+s2.i)) AS j FROM integers i1 ORDER BY i;

SELECT i, SUM(i), (SELECT (SELECT SUM(i)+SUM(i1.i)+SUM(i2.i) FROM integers) FROM integers i2) FROM integers i1 GROUP BY i ORDER BY i;

SELECT i, (SELECT SUM(ss1.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(s1.i) FROM integers s1 LEFT OUTER JOIN integers s2 ON (SELECT i1.i+s1.i)=(SELECT i1.i+s2.i)) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(s1.i) FROM (SELECT i FROM integers WHERE i=i1.i) s1 LEFT OUTER JOIN integers s2 ON s1.i=s2.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(s1.i) FROM (SELECT i FROM integers WHERE i<>i1.i) s1 LEFT OUTER JOIN integers s2 ON s1.i=s2.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(s2.i) FROM integers s1 LEFT OUTER JOIN (SELECT i FROM integers WHERE i=i1.i) s2 ON s1.i=s2.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(s2.i) FROM integers s1 LEFT OUTER JOIN (SELECT i FROM integers WHERE i<>i1.i) s2 ON s1.i=s2.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE CASE WHEN (i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) THEN true ELSE false END) ss2) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i) ss2) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT i=ANY(SELECT i FROM integers WHERE i=s1.i) FROM integers s1 WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i OR i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE CASE WHEN (i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) THEN true ELSE false END) ss2) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND EXISTS(SELECT i FROM integers WHERE i=s1.i)) ss2) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss1.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss1.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i<>i1.i OR i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT * FROM (SELECT (SELECT 42+i1.i)) s1) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT s1.k+s2.k FROM (SELECT (SELECT 42+i1.i) AS k) s1, (SELECT (SELECT 42+i1.i) AS k) s2) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT s1.k+s2.k FROM (SELECT (SELECT 42+i1.i) AS k) s1 LEFT OUTER JOIN (SELECT (SELECT 42+i1.i) AS k) s2 ON s1.k=s2.k) AS j FROM integers i1 ORDER BY i;

SELECT i, (SELECT i1.i IN (1, 2, 3, 4, 5, 6, 7, 8)) AS j FROM integers i1 ORDER BY i;

SELECT (SELECT (SELECT COVAR_POP(i1.i, i3.i) FROM integers i3) FROM integers i2 LIMIT 1) FROM integers i1 ORDER BY 1;

SELECT (SELECT (SELECT COVAR_POP(i2.i, i3.i) FROM integers i3) FROM integers i2 ORDER BY i NULLS LAST LIMIT 1) FROM integers i1 ORDER BY 1;