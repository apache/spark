set hive.stats.autogather=false;
set hive.optimize.union.remove=true;
set hive.mapred.supports.subdirectories=true;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set mapred.input.dir.recursive=true;

-- This is to test the union->selectstar->filesink optimization
-- Union of 2 map-reduce subqueries is performed followed by select and a file sink
-- However, the order of the columns in the select list is different. So, union cannot
-- be removed.
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- off
-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- Since this test creates sub-directories for the output table outputTbl1, it might be easier
-- to run the test only on hadoop 23. The union is removed, the select (which changes the order of
-- columns being selected) is pushed above the union.

create table inputTbl1(key string, val string) stored as textfile;
create table outputTbl1(values bigint, key string) stored as textfile;

load data local inpath '../../data/files/T1.txt' into table inputTbl1;

explain
insert overwrite table outputTbl1
SELECT a.values, a.key
FROM (
  SELECT key, count(1) as values from inputTbl1 group by key
  UNION ALL
  SELECT key, count(1) as values from inputTbl1 group by key
) a;

insert overwrite table outputTbl1
SELECT a.values, a.key
FROM (
  SELECT key, count(1) as values from inputTbl1 group by key
  UNION ALL
  SELECT key, count(1) as values from inputTbl1 group by key
) a;

desc formatted outputTbl1;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1 order by key, values;
