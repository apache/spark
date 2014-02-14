set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.min.split.size=256;
set mapred.min.split.size.per.node=256;
set mapred.min.split.size.per.rack=256;
set mapred.max.split.size=256;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.cache.shared.enabled=false;
set hive.merge.smallfiles.avgsize=0;

-- INCLUDE_OS_WINDOWS
-- included only on  windows because of difference in file name encoding logic

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.20, 0.20S)

create table combine2(key string) partitioned by (value string);

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
