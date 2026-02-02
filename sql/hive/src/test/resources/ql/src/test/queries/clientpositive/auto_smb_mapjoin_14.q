set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;

CREATE TABLE tbl1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

insert overwrite table tbl1
select * from src where key < 10;

insert overwrite table tbl2
select * from src where key < 10;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

set hive.auto.convert.sortmerge.join=true;

-- The join is being performed as part of sub-query. It should be converted to a sort-merge join
explain
select count(*) from (
  select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
) subq1;

select count(*) from (
  select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
) subq1;

-- The join is being performed as part of more than one sub-query. It should be converted to a sort-merge join
explain
select count(*) from
(
  select key, count(*) from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
  ) subq1
  group by key
) subq2;

select count(*) from
(
  select key, count(*) from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
  ) subq1
  group by key
) subq2;

-- A join is being performed across different sub-queries, where a join is being performed in each of them.
-- Each sub-query should be converted to a sort-merge join.
explain
select src1.key, src1.cnt1, src2.cnt1 from
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
  ) subq1 group by key
) src1
join
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
  ) subq2 group by key
) src2
on src1.key = src2.key
order by src1.key, src1.cnt1, src2.cnt1;

select src1.key, src1.cnt1, src2.cnt1 from
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
  ) subq1 group by key
) src1
join
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
  ) subq2 group by key
) src2
on src1.key = src2.key
order by src1.key, src1.cnt1, src2.cnt1;

-- The subquery itself is being joined. Since the sub-query only contains selects and filters, it should 
-- be converted to a sort-merge join.
explain
select count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on subq1.key = subq2.key;

-- The subquery itself is being joined. Since the sub-query only contains selects and filters, it should 
-- be converted to a sort-merge join, although there is more than one level of sub-query
explain
select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join tbl2 b
  on subq2.key = b.key;

select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join tbl2 b
  on subq2.key = b.key;

-- Both the tables are nested sub-queries i.e more then 1 level of sub-query.
-- The join should be converted to a sort-merge join
explain
select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1 a where key < 8
    ) subq3 
  where key < 6
  ) subq4
  on subq2.key = subq4.key;

select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1 a where key < 8
    ) subq3 
  where key < 6
  ) subq4
  on subq2.key = subq4.key;

-- The subquery itself is being joined. Since the sub-query only contains selects and filters and the join key
-- is not getting modified, it should be converted to a sort-merge join. Note that the sub-query modifies one 
-- item, but that is not part of the join key.
explain
select count(*) from 
  (select a.key as key, concat(a.value, a.value) as value from tbl1 a where key < 8) subq1 
    join
  (select a.key as key, concat(a.value, a.value) as value from tbl2 a where key < 8) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (select a.key as key, concat(a.value, a.value) as value from tbl1 a where key < 8) subq1 
    join
  (select a.key as key, concat(a.value, a.value) as value from tbl2 a where key < 8) subq2
  on subq1.key = subq2.key;

-- Since the join key is modified by the sub-query, neither sort-merge join not bucketized map-side
-- join should be performed
explain
select count(*) from 
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl1 a) subq1 
    join
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl2 a) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl1 a) subq1 
    join
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl2 a) subq2
  on subq1.key = subq2.key;

-- One of the tables is a sub-query and the other is not.
-- It should be converted to a sort-merge join.
explain
select count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join tbl2 a on subq1.key = a.key;

select count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join tbl2 a on subq1.key = a.key;

-- There are more than 2 inputs to the join, all of them being sub-queries. 
-- It should be converted to to a sort-merge join
explain
select count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on (subq1.key = subq2.key)
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq3
  on (subq1.key = subq3.key);

select count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on subq1.key = subq2.key
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq3
  on (subq1.key = subq3.key);

-- The join is being performed on a nested sub-query, and an aggregation is performed after that.
-- The join should be converted to a sort-merge join
explain
select count(*) from (
  select subq2.key as key, subq2.value as value1, b.value as value2 from
  (
    select * from
    (
      select a.key as key, a.value as value from tbl1 a where key < 8
    ) subq1
    where key < 6
  ) subq2
join tbl2 b
on subq2.key = b.key) a;

select count(*) from (
  select subq2.key as key, subq2.value as value1, b.value as value2 from
  (
    select * from
    (
      select a.key as key, a.value as value from tbl1 a where key < 8
    ) subq1
    where key < 6
  ) subq2
join tbl2 b
on subq2.key = b.key) a;

CREATE TABLE dest1(key int, value string);
CREATE TABLE dest2(key int, val1 string, val2 string);

-- The join is followed by a multi-table insert. It should be converted to
-- a sort-merge join
explain
from (
  select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
) subq1
insert overwrite table dest1 select key, val1
insert overwrite table dest2 select key, val1, val2;

from (
  select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
) subq1
insert overwrite table dest1 select key, val1
insert overwrite table dest2 select key, val1, val2;

select * from dest1 order by key, value;
select * from dest2 order by key, val1, val2;

DROP TABLE dest2;
CREATE TABLE dest2(key int, cnt int);

-- The join is followed by a multi-table insert, and one of the inserts involves a reducer.
-- It should be converted to a sort-merge join
explain
from (
  select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
) subq1
insert overwrite table dest1 select key, val1
insert overwrite table dest2 select key, count(*) group by key;

from (
  select a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
) subq1
insert overwrite table dest1 select key, val1
insert overwrite table dest2 select key, count(*) group by key;

select * from dest1 order by key, value;
select * from dest2 order by key;
