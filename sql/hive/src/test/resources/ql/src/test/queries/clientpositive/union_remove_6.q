set hive.stats.autogather=false;
set hive.optimize.union.remove=true;
set hive.mapred.supports.subdirectories=true;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set mapred.input.dir.recursive=true;

-- This is to test the union->selectstar->filesink optimization
-- Union of 2 subqueries is performed (all of which are mapred queries)
-- followed by select star and a file sink in 2 output tables.
-- The optimiaztion does not take affect since it is a multi-table insert.
-- It does not matter, whether the output is merged or not. In this case,
-- merging is turned off

create table inputTbl1(key string, val string) stored as textfile;
create table outputTbl1(key string, values bigint) stored as textfile;
create table outputTbl2(key string, values bigint) stored as textfile;

load data local inpath '../data/files/T1.txt' into table inputTbl1;

explain
FROM (
  SELECT key, count(1) as values from inputTbl1 group by key
  UNION ALL
  SELECT key, count(1) as values from inputTbl1 group by key
) a
insert overwrite table outputTbl1 select *
insert overwrite table outputTbl2 select *;

FROM (
  SELECT key, count(1) as values from inputTbl1 group by key
  UNION ALL
  SELECT key, count(1) as values from inputTbl1 group by key
) a
insert overwrite table outputTbl1 select *
insert overwrite table outputTbl2 select *;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1 order by key, values;
select * from outputTbl2 order by key, values;;
