CREATE TABLE tmp(c1 INT, c2 INT, c3 STRING, c4 STRING);
INSERT OVERWRITE TABLE tmp
SELECT x.key, y.key, x.value, y.value FROM src x JOIN src y ON (x.key = y.key);

set hive.optimize.correlation=true;
-- The query in this file have operators with same set of keys
-- but having different sorting orders.
-- Correlation optimizer currently do not optimize this case.
-- This case will be optimized latter (need a follow-up jira).

EXPLAIN
SELECT xx.key1, xx.key2, yy.key1, yy.key2, xx.cnt, yy.cnt
FROM 
(SELECT x.c1 AS key1, x.c3 AS key2, count(1) AS cnt FROM tmp x WHERE x.c1 < 120 GROUP BY x.c3, x.c1) xx
JOIN
(SELECT x1.c1 AS key1, x1.c3 AS key2, count(1) AS cnt FROM tmp x1 WHERE x1.c2 > 100 GROUP BY x1.c3, x1.c1) yy
ON (xx.key1 = yy.key1 AND xx.key2 == yy.key2) ORDER BY xx.key1, xx.key2, yy.key1, yy.key2, xx.cnt, yy.cnt;

