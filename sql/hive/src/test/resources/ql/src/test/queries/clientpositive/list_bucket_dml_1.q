set hive.mapred.supports.subdirectories=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.merge.mapfiles=false;	
set hive.merge.mapredfiles=false; 
set mapred.input.dir.recursive=true;

-- list bucketing DML : dynamic partition and 2 stage query plan.

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)

-- create a skewed table
create table list_bucketing_dynamic_part (key String, value String) 
partitioned by (ds String, hr String) 
skewed by (key) on ("484")
stored as DIRECTORIES
;

-- list bucketing DML
explain extended
insert overwrite table list_bucketing_dynamic_part partition (ds='2008-04-08', hr) select key, value, hr from srcpart where ds='2008-04-08';
insert overwrite table list_bucketing_dynamic_part partition (ds='2008-04-08', hr) select key, value, hr from srcpart where ds='2008-04-08';

-- check DML result
desc formatted list_bucketing_dynamic_part partition (ds='2008-04-08', hr='11');
desc formatted list_bucketing_dynamic_part partition (ds='2008-04-08', hr='12');

select count(1) from srcpart where ds='2008-04-08';
select count(1) from list_bucketing_dynamic_part where ds='2008-04-08';

select key, value from srcpart where ds='2008-04-08' and hr='11' and key = "484";
set hive.optimize.listbucketing=true;
explain extended
select key, value from list_bucketing_dynamic_part where ds='2008-04-08' and hr='11' and key = "484";
select key, value from list_bucketing_dynamic_part where ds='2008-04-08' and hr='11' and key = "484";

-- clean up resources
drop table list_bucketing_dynamic_part;

