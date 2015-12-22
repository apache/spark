-- simple query with multiple reduce stages
EXPLAIN SELECT key, count(value) as cnt FROM src GROUP BY key ORDER BY cnt;
SELECT key, count(value) as cnt FROM src GROUP BY key ORDER BY cnt;

set hive.auto.convert.join=false;
-- join query with multiple reduce stages;
EXPLAIN SELECT s2.key, count(distinct s2.value) as cnt FROM src s1 join src s2 on (s1.key = s2.key) GROUP BY s2.key ORDER BY cnt;
SELECT s2.key, count(distinct s2.value) as cnt FROM src s1 join src s2 on (s1.key = s2.key) GROUP BY s2.key ORDER BY cnt;

set hive.auto.convert.join=true;
-- same query with broadcast join
EXPLAIN SELECT s2.key, count(distinct s2.value) as cnt FROM src s1 join src s2 on (s1.key = s2.key) GROUP BY s2.key ORDER BY cnt;
SELECT s2.key, count(distinct s2.value) as cnt FROM src s1 join src s2 on (s1.key = s2.key) GROUP BY s2.key ORDER BY cnt;

set hive.auto.convert.join=false;
-- query with multiple branches in the task dag
EXPLAIN
SELECT * 
FROM
  (SELECT key, count(value) as cnt 
  FROM src GROUP BY key ORDER BY cnt) s1
  JOIN
  (SELECT key, count(value) as cnt 
  FROM src GROUP BY key ORDER BY cnt) s2
  JOIN
  (SELECT key, count(value) as cnt 
  FROM src GROUP BY key ORDER BY cnt) s3
  ON (s1.key = s2.key and s1.key = s3.key)
WHERE
  s1.cnt > 1
ORDER BY s1.key;

SELECT * 
FROM
  (SELECT key, count(value) as cnt 
  FROM src GROUP BY key ORDER BY cnt) s1
  JOIN
  (SELECT key, count(value) as cnt 
  FROM src GROUP BY key ORDER BY cnt) s2
  JOIN
  (SELECT key, count(value) as cnt 
  FROM src GROUP BY key ORDER BY cnt) s3
  ON (s1.key = s2.key and s1.key = s3.key)
WHERE
  s1.cnt > 1
ORDER BY s1.key;

set hive.auto.convert.join=true;
-- query with broadcast join in the reduce stage
EXPLAIN
SELECT *
FROM
  (SELECT key, count(value) as cnt FROM src GROUP BY key) s1
  JOIN src ON (s1.key = src.key);

SELECT *
FROM
  (SELECT key, count(value) as cnt FROM src GROUP BY key) s1
  JOIN src ON (s1.key = src.key);
