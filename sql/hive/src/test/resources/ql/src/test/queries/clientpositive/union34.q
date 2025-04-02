create table src10_1 (key string, value string);
create table src10_2 (key string, value string);
create table src10_3 (key string, value string);
create table src10_4 (key string, value string);

from (select * from src tablesample (10 rows)) a
insert overwrite table src10_1 select *
insert overwrite table src10_2 select *
insert overwrite table src10_3 select *
insert overwrite table src10_4 select *;

set hive.auto.convert.join=true;
-- When we convert the Join of sub1 and sub0 into a MapJoin,
-- we can use a single MR job to evaluate this entire query.
explain
SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION ALL
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION ALL SELECT * FROM src10_4 ) alias0
) alias1 order by key;

SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION ALL
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION ALL SELECT * FROM src10_4 ) alias0
) alias1 order by key;

set hive.auto.convert.join=false;
-- When we do not convert the Join of sub1 and sub0 into a MapJoin,
-- we need to use two MR jobs to evaluate this query.
-- The first job is for the Join of sub1 and sub2. The second job
-- is for the UNION ALL and ORDER BY.
explain
SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION ALL
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION ALL SELECT * FROM src10_4 ) alias0
) alias1 order by key;

SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION ALL
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION ALL SELECT * FROM src10_4 ) alias0
) alias1 order by key;
