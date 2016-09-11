set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;

CREATE TABLE tbl1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

insert overwrite table tbl1
select * from src where key < 10;

insert overwrite table tbl2
select * from src where key < 10;

set hive.auto.convert.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

set hive.auto.convert.sortmerge.join=true;
set hive.auto.convert.sortmerge.join.to.mapjoin=false;
-- One of the subqueries contains a union, so it should not be converted to a sort-merge join.
explain
select count(*) from 
  (
  select * from
  (select a.key as key, a.value as value from tbl1 a where key < 6
     union all
   select a.key as key, a.value as value from tbl1 a where key < 6
  ) usubq1 ) subq1
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (
  select * from
  (select a.key as key, a.value as value from tbl1 a where key < 6
     union all
   select a.key as key, a.value as value from tbl1 a where key < 6
  ) usubq1 ) subq1
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on subq1.key = subq2.key;

-- One of the subqueries contains a groupby, so it should not be converted to a sort-merge join.
explain
select count(*) from 
  (select a.key as key, count(*) as value from tbl1 a where key < 6 group by a.key) subq1 
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (select a.key as key, count(*) as value from tbl1 a where key < 6 group by a.key) subq1 
    join
  (select a.key as key, a.value as value from tbl2 a where key < 6) subq2
  on subq1.key = subq2.key;
