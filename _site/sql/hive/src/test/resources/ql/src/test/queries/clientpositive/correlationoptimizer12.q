set hive.auto.convert.join=false;
set hive.optimize.correlation=true;
-- Currently, correlation optimizer does not support PTF operator
EXPLAIN SELECT xx.key, xx.cnt, yy.key, yy.cnt
FROM
(SELECT x.key as key, count(x.value) OVER (PARTITION BY x.key) AS cnt FROM src x) xx
JOIN
(SELECT y.key as key, count(y.value) OVER (PARTITION BY y.key) AS cnt FROM src1 y) yy
ON (xx.key=yy.key);
