set hive.mapred.supports.subdirectories=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.merge.smallfiles.avgsize=200;
set mapred.input.dir.recursive=true;
set hive.merge.mapfiles=false;	
set hive.merge.mapredfiles=false;
set hive.merge.rcfile.block.level=true;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)

-- list bucketing DML : dynamic partition (one level) , merge , one skewed column
-- DML without merge files mixed with small and big files:
-- ds=2008-04-08/hr=a1/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/
-- 155 000000_0
-- ds=2008-04-08/hr=b1/key=484
-- 87 000000_0
-- 87 000001_0
-- ds=2008-04-08/hr=b1/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/
-- 5201 000000_0
-- 5201 000001_0
-- DML with merge will merge small files

-- skewed table
CREATE TABLE list_bucketing_dynamic_part (key String, value STRING)
    PARTITIONED BY (ds string, hr string)
    skewed by (key) on ('484')
    stored as DIRECTORIES
    STORED AS RCFILE;
   
-- list bucketing DML without merge. use bucketize to generate a few small files.
explain extended
insert overwrite table list_bucketing_dynamic_part partition (ds = '2008-04-08', hr)
select key, value, if(key % 100 == 0, 'a1', 'b1') from srcpart where ds = '2008-04-08';

insert overwrite table list_bucketing_dynamic_part partition (ds = '2008-04-08', hr)
select key, value, if(key % 100 == 0, 'a1', 'b1') from srcpart where ds = '2008-04-08';

-- check DML result
show partitions list_bucketing_dynamic_part;
desc formatted list_bucketing_dynamic_part partition (ds='2008-04-08', hr='a1');	
desc formatted list_bucketing_dynamic_part partition (ds='2008-04-08', hr='b1');

set hive.merge.mapfiles=true;	
set hive.merge.mapredfiles=true; 
-- list bucketing DML with merge. use bucketize to generate a few small files.
explain extended
insert overwrite table list_bucketing_dynamic_part partition (ds = '2008-04-08', hr)
select key, value, if(key % 100 == 0, 'a1', 'b1') from srcpart where ds = '2008-04-08';

insert overwrite table list_bucketing_dynamic_part partition (ds = '2008-04-08', hr)
select key, value, if(key % 100 == 0, 'a1', 'b1') from srcpart where ds = '2008-04-08';

-- check DML result
show partitions list_bucketing_dynamic_part;
desc formatted list_bucketing_dynamic_part partition (ds='2008-04-08', hr='a1');	
desc formatted list_bucketing_dynamic_part partition (ds='2008-04-08', hr='b1');

select count(1) from srcpart where ds = '2008-04-08';
select count(*) from list_bucketing_dynamic_part;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
explain extended
select * from list_bucketing_dynamic_part where key = '484' and value = 'val_484';
select * from list_bucketing_dynamic_part where key = '484' and value = 'val_484';
select * from srcpart where ds = '2008-04-08' and key = '484' and value = 'val_484' order by hr;

-- clean up
drop table list_bucketing_dynamic_part;
