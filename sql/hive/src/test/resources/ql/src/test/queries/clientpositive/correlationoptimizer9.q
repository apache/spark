CREATE TABLE tmp(c1 INT, c2 INT, c3 STRING, c4 STRING);

set hive.auto.convert.join=false;

INSERT OVERWRITE TABLE tmp
SELECT x.key, y.key, x.value, y.value FROM src x JOIN src y ON (x.key = y.key);

set hive.optimize.correlation=false;
EXPLAIN
SELECT xx.key, yy.key, xx.cnt, yy.cnt
FROM 
(SELECT x.c1 AS key, count(1) AS cnt FROM tmp x WHERE x.c1 < 120 GROUP BY x.c1) xx
JOIN
(SELECT x1.c2 AS key, count(1) AS cnt FROM tmp x1 WHERE x1.c2 > 100 GROUP BY x1.c2) yy
ON (xx.key = yy.key) ORDER BY xx.key, yy.key, xx.cnt, yy.cnt;

SELECT xx.key, yy.key, xx.cnt, yy.cnt
FROM 
(SELECT x.c1 AS key, count(1) AS cnt FROM tmp x WHERE x.c1 < 120 GROUP BY x.c1) xx
JOIN
(SELECT x1.c2 AS key, count(1) AS cnt FROM tmp x1 WHERE x1.c2 > 100 GROUP BY x1.c2) yy
ON (xx.key = yy.key) ORDER BY xx.key, yy.key, xx.cnt, yy.cnt;

set hive.optimize.correlation=true;
-- The merged table scan should be able to load both c1 and c2
EXPLAIN
SELECT xx.key, yy.key, xx.cnt, yy.cnt
FROM 
(SELECT x.c1 AS key, count(1) AS cnt FROM tmp x WHERE x.c1 < 120 GROUP BY x.c1) xx
JOIN
(SELECT x1.c2 AS key, count(1) AS cnt FROM tmp x1 WHERE x1.c2 > 100 GROUP BY x1.c2) yy
ON (xx.key = yy.key) ORDER BY xx.key, yy.key, xx.cnt, yy.cnt;

SELECT xx.key, yy.key, xx.cnt, yy.cnt
FROM 
(SELECT x.c1 AS key, count(1) AS cnt FROM tmp x WHERE x.c1 < 120 GROUP BY x.c1) xx
JOIN
(SELECT x1.c2 AS key, count(1) AS cnt FROM tmp x1 WHERE x1.c2 > 100 GROUP BY x1.c2) yy
ON (xx.key = yy.key) ORDER BY xx.key, yy.key, xx.cnt, yy.cnt;

set hive.optimize.correlation=false;
EXPLAIN
SELECT xx.key1, xx.key2, yy.key1, yy.key2, xx.cnt, yy.cnt
FROM 
(SELECT x.c1 AS key1, x.c3 AS key2, count(1) AS cnt FROM tmp x WHERE x.c1 < 120 GROUP BY x.c1, x.c3) xx
JOIN
(SELECT x1.c1 AS key1, x1.c3 AS key2, count(1) AS cnt FROM tmp x1 WHERE x1.c2 > 100 GROUP BY x1.c1, x1.c3) yy
ON (xx.key1 = yy.key1 AND xx.key2 == yy.key2) ORDER BY xx.key1, xx.key2, yy.key1, yy.key2, xx.cnt, yy.cnt;

SELECT xx.key1, xx.key2, yy.key1, yy.key2, xx.cnt, yy.cnt
FROM 
(SELECT x.c1 AS key1, x.c3 AS key2, count(1) AS cnt FROM tmp x WHERE x.c1 < 120 GROUP BY x.c1, x.c3) xx
JOIN
(SELECT x1.c1 AS key1, x1.c3 AS key2, count(1) AS cnt FROM tmp x1 WHERE x1.c2 > 100 GROUP BY x1.c1, x1.c3) yy
ON (xx.key1 = yy.key1 AND xx.key2 == yy.key2) ORDER BY xx.key1, xx.key2, yy.key1, yy.key2, xx.cnt, yy.cnt;

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key1, xx.key2, yy.key1, yy.key2, xx.cnt, yy.cnt
FROM 
(SELECT x.c1 AS key1, x.c3 AS key2, count(1) AS cnt FROM tmp x WHERE x.c1 < 120 GROUP BY x.c1, x.c3) xx
JOIN
(SELECT x1.c1 AS key1, x1.c3 AS key2, count(1) AS cnt FROM tmp x1 WHERE x1.c2 > 100 GROUP BY x1.c1, x1.c3) yy
ON (xx.key1 = yy.key1 AND xx.key2 == yy.key2) ORDER BY xx.key1, xx.key2, yy.key1, yy.key2, xx.cnt, yy.cnt;

SELECT xx.key1, xx.key2, yy.key1, yy.key2, xx.cnt, yy.cnt
FROM 
(SELECT x.c1 AS key1, x.c3 AS key2, count(1) AS cnt FROM tmp x WHERE x.c1 < 120 GROUP BY x.c1, x.c3) xx
JOIN
(SELECT x1.c1 AS key1, x1.c3 AS key2, count(1) AS cnt FROM tmp x1 WHERE x1.c2 > 100 GROUP BY x1.c1, x1.c3) yy
ON (xx.key1 = yy.key1 AND xx.key2 == yy.key2) ORDER BY xx.key1, xx.key2, yy.key1, yy.key2, xx.cnt, yy.cnt;
