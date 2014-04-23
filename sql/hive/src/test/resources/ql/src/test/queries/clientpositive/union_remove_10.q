set hive.stats.autogather=false;
set hive.optimize.union.remove=true;
set hive.mapred.supports.subdirectories=true;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=1;
set mapred.input.dir.recursive=true;

-- This is to test the union->selectstar->filesink optimization
-- Union of 2 subqueries is performed (one of which is a map-only query, and the
-- other one contains a nested union where one of the sub-queries requires a map-reduce
-- job), followed by select star and a file sink.
-- There is no need to write the temporary results of the sub-queries, and then read them 
-- again to process the union. The outer union can be removed completely.
-- The final file format is different from the input and intermediate file format.
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- on

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- Since this test creates sub-directories for the output table outputTbl1, it might be easier
-- to run the test only on hadoop 23

create table inputTbl1(key string, val string) stored as textfile;
create table outputTbl1(key string, values bigint) stored as rcfile;

load data local inpath '../data/files/T1.txt' into table inputTbl1;

explain
insert overwrite table outputTbl1
SELECT * FROM
(
select key, 1 as values from inputTbl1
union all
select * FROM (
  SELECT key, count(1) as values from inputTbl1 group by key
  UNION ALL
  SELECT key, 2 as values from inputTbl1
) a
)b;

insert overwrite table outputTbl1
SELECT * FROM
(
select key, 1 as values from inputTbl1
union all
select * FROM (
  SELECT key, count(1) as values from inputTbl1 group by key
  UNION ALL
  SELECT key, 2 as values from inputTbl1
) a
)b;

desc formatted outputTbl1;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1 order by key, values;
