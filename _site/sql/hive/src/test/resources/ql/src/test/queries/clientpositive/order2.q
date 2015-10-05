set hive.optimize.ppd=true;

EXPLAIN
SELECT subq.key, subq.value FROM 
(SELECT x.* FROM SRC x ORDER BY key limit 10) subq
where subq.key < 10;

SELECT subq.key, subq.value FROM 
(SELECT x.* FROM SRC x ORDER BY key limit 10) subq
where subq.key < 10;
