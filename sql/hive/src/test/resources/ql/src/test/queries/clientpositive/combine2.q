USE default;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.minsize=256;
set mapreduce.input.fileinputformat.split.minsize.per.node=256;
set mapreduce.input.fileinputformat.split.minsize.per.rack=256;
set mapreduce.input.fileinputformat.split.maxsize=256;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.cache.shared.enabled=false;
set hive.merge.smallfiles.avgsize=0;

-- EXCLUDE_OS_WINDOWS
-- excluded on windows because of difference in file name encoding logic



create table combine2(key string) partitioned by (value string);

-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.20, 0.20S)
-- This test sets mapreduce.input.fileinputformat.split.maxsize=256 and hive.merge.smallfiles.avgsize=0
-- in an attempt to force the generation of multiple splits and multiple output files.
-- However, Hadoop 0.20 is incapable of generating splits smaller than the block size
-- when using CombineFileInputFormat, so only one split is generated. This has a
-- significant impact on the results results of this test.
-- This issue was fixed in MAPREDUCE-2046 which is included in 0.22.

insert overwrite table combine2 partition(value) 
select * from (
   select key, value from src where key < 10
   union all 
   select key, '|' as value from src where key = 11
   union all
   select key, '2010-04-21 09:45:00' value from src where key = 19) s;

show partitions combine2;

explain
select key, value from combine2 where value is not null order by key;

select key, value from combine2 where value is not null order by key;

explain extended
select count(1) from combine2 where value is not null;

select count(1) from combine2 where value is not null;

explain
select ds, count(1) from srcpart where ds is not null group by ds;

select ds, count(1) from srcpart where ds is not null group by ds;
