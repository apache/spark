set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;

explain
select b.key,b.cc
from (
  select a.*
  from (
    select key, count(value) as cc
    from srcpart a
    where a.ds = '2008-04-08' and a.hr = '11'
    group by key
  )a
  distribute by a.key
  sort by a.key,a.cc desc) b
where b.cc>1;

select b.key,b.cc
from (
  select a.*
  from (
    select key, count(value) as cc
    from srcpart a
    where a.ds = '2008-04-08' and a.hr = '11'
    group by key
  )a
  distribute by a.key
  sort by a.key,a.cc desc) b
where b.cc>1;

EXPLAIN
SELECT user_id 
FROM (
  SELECT 
  CAST(key AS INT) AS user_id
  ,CASE WHEN (value LIKE 'aaa%' OR value LIKE 'vvv%')
  THEN 1
  ELSE 0 END AS tag_student
  FROM srcpart
) sub
WHERE sub.tag_student > 0;

EXPLAIN 
SELECT x.key, x.value as v1, y.key  FROM SRC x JOIN SRC y ON (x.key = y.key)  where x.key = 20 CLUSTER BY v1;

set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

explain
select b.key,b.cc
from (
  select a.*
  from (
    select key, count(value) as cc
    from srcpart a
    where a.ds = '2008-04-08' and a.hr = '11'
    group by key
  )a
  distribute by a.key
  sort by a.key,a.cc desc) b
where b.cc>1;

select b.key,b.cc
from (
  select a.*
  from (
    select key, count(value) as cc
    from srcpart a
    where a.ds = '2008-04-08' and a.hr = '11'
    group by key
  )a
  distribute by a.key
  sort by a.key,a.cc desc) b
where b.cc>1;
