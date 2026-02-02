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

-- The mapjoin is being performed as part of sub-query. It should be converted to a sort-merge join
explain
select count(*) from (
  select /*+mapjoin(a)*/ a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
) subq1;

select count(*) from (
  select /*+mapjoin(a)*/ a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
) subq1;

-- The mapjoin is being performed as part of sub-query. It should be converted to a sort-merge join
-- Add a order by at the end to make the results deterministic.
explain
select key, count(*) from 
(
  select /*+mapjoin(a)*/ a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
) subq1
group by key
order by key;

select key, count(*) from 
(
  select /*+mapjoin(a)*/ a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
) subq1
group by key
order by key;

-- The mapjoin is being performed as part of more than one sub-query. It should be converted to a sort-merge join
explain
select count(*) from
(
  select key, count(*) from 
  (
    select /*+mapjoin(a)*/ a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
  ) subq1
  group by key
) subq2;

select count(*) from
(
  select key, count(*) from 
  (
    select /*+mapjoin(a)*/ a.key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key
  ) subq1
  group by key
) subq2;

-- The subquery itself is being map-joined. Since the sub-query only contains selects and filters, it should 
-- be converted to a sort-merge join.
explain
select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on subq1.key = subq2.key;

select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on subq1.key = subq2.key;

-- The subquery itself is being map-joined. Since the sub-query only contains selects and filters, it should 
-- be converted to a sort-merge join, although there is more than one level of sub-query
explain
select /*+mapjoin(subq2)*/ count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join tbl2 b
  on subq2.key = b.key;

select /*+mapjoin(subq2)*/ count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join tbl2 b
  on subq2.key = b.key;

-- Both the big table and the small table are nested sub-queries i.e more then 1 level of sub-query.
-- The join should be converted to a sort-merge join
explain
select /*+mapjoin(subq2)*/ count(*) from 
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

select /*+mapjoin(subq2)*/ count(*) from 
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

-- The subquery itself is being map-joined. Since the sub-query only contains selects and filters and the join key
-- is not getting modified, it should be converted to a sort-merge join. Note that the sub-query modifies one 
-- item, but that is not part of the join key.
explain
select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key as key, concat(a.value, a.value) as value from tbl1 a where key < 8) subq1 
    join
  (select a.key as key, concat(a.value, a.value) as value from tbl2 a where key < 8) subq2
  on subq1.key = subq2.key;

select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key as key, concat(a.value, a.value) as value from tbl1 a where key < 8) subq1 
    join
  (select a.key as key, concat(a.value, a.value) as value from tbl2 a where key < 8) subq2
  on subq1.key = subq2.key;

-- Since the join key is modified by the sub-query, neither sort-merge join not bucketized map-side
-- join should be performed
explain
select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl1 a) subq1 
    join
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl2 a) subq2
  on subq1.key = subq2.key;

select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl1 a) subq1 
    join
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl2 a) subq2
  on subq1.key = subq2.key;

-- The small table is a sub-query and the big table is not.
-- It should be converted to a sort-merge join.
explain
select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join tbl2 a on subq1.key = a.key;

select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join tbl2 a on subq1.key = a.key;

-- The big table is a sub-query and the small table is not.
-- It should be converted to a sort-merge join.
explain
select /*+mapjoin(a)*/ count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join tbl2 a on subq1.key = a.key;

select /*+mapjoin(a)*/ count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join tbl2 a on subq1.key = a.key;

-- There are more than 2 inputs to the join, all of them being sub-queries. 
-- It should be converted to to a sort-merge join
explain
select /*+mapjoin(subq1, subq2)*/ count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on (subq1.key = subq2.key)
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq3
  on (subq1.key = subq3.key);

select /*+mapjoin(subq1, subq2)*/ count(*) from 
  (select a.key as key, a.value as value from tbl1 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on subq1.key = subq2.key
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq3
  on (subq1.key = subq3.key);

-- The mapjoin is being performed on a nested sub-query, and an aggregation is performed after that.
-- The join should be converted to a sort-merge join
explain
select count(*) from (
  select /*+mapjoin(subq2)*/ subq2.key as key, subq2.value as value1, b.value as value2 from
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
  select /*+mapjoin(subq2)*/ subq2.key as key, subq2.value as value1, b.value as value2 from
  (
    select * from
    (
      select a.key as key, a.value as value from tbl1 a where key < 8
    ) subq1
    where key < 6
  ) subq2
join tbl2 b
on subq2.key = b.key) a;
