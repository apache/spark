set hive.stats.autogather=false;
set hive.optimize.union.remove=true;
set hive.mapred.supports.subdirectories=true;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set mapred.input.dir.recursive=true;

-- This is to test the union->remove->filesink optimization
-- Union of 3 subqueries is performed (all of which are map-only queries)
-- followed by select star and a file sink.
-- There is no need for any optimization, since the whole query can be processed in
-- a single map-only job
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- off
-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- Since this test creates sub-directories for the output table outputTbl1, it might be easier
-- to run the test only on hadoop 23

create table inputTbl1(key string, val string) stored as textfile;
create table outputTbl1(key string, values bigint) stored as textfile;

load data local inpath '../../data/files/T1.txt' into table inputTbl1;

explain
insert overwrite table outputTbl1
SELECT *
FROM (
  SELECT key, 1 as values from inputTbl1
  UNION ALL
  SELECT key, 2 as values from inputTbl1
  UNION ALL
  SELECT key, 3 as values from inputTbl1
) a;

insert overwrite table outputTbl1
SELECT *
FROM (
  SELECT key, 1 as values from inputTbl1
  UNION ALL
  SELECT key, 2 as values from inputTbl1
  UNION ALL
  SELECT key, 3 as values from inputTbl1
) a;

desc formatted outputTbl1;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1 order by key, values;

