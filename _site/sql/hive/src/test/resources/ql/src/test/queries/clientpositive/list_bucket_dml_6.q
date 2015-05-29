set hive.mapred.supports.subdirectories=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.merge.smallfiles.avgsize=200;
set mapred.input.dir.recursive=true;
set hive.merge.mapfiles=false;	
set hive.merge.mapredfiles=false;

-- list bucketing DML: dynamic partition. multiple skewed columns. merge.
-- The following explains merge example used in this test case
-- DML will generated 2 partitions
-- ds=2008-04-08/hr=a1
-- ds=2008-04-08/hr=b1
-- without merge, each partition has more files
-- ds=2008-04-08/hr=a1 has 2 files
-- ds=2008-04-08/hr=b1 has 6 files
-- with merge each partition has more files
-- ds=2008-04-08/hr=a1 has 1 files
-- ds=2008-04-08/hr=b1 has 4 files
-- The following shows file size and name in each directory
-- hr=a1/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME:
-- without merge
-- 155 000000_0
-- 155 000001_0
-- with merge
-- 254 000000_0
-- hr=b1/key=103/value=val_103:
-- without merge
-- 99 000000_0
-- 99 000001_0
-- with merge
-- 142 000001_0
-- hr=b1/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME:
-- without merge
-- 5181 000000_0
-- 5181 000001_0
-- with merge
-- 5181 000000_0
-- 5181 000001_0
-- hr=b1/key=484/value=val_484
-- without merge
-- 87 000000_0
-- 87 000001_0
-- with merge
-- 118 000002_0 

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)

-- create a skewed table
create table list_bucketing_dynamic_part (key String, value String) 
    partitioned by (ds String, hr String) 
    skewed by (key, value) on (('484','val_484'),('51','val_14'),('103','val_103'))
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
set hive.optimize.listbucketing=true;
explain extended
select * from list_bucketing_dynamic_part where key = '484' and value = 'val_484';
select * from list_bucketing_dynamic_part where key = '484' and value = 'val_484';
select * from srcpart where ds = '2008-04-08' and key = '484' and value = 'val_484' order by key, value, ds, hr;

-- clean up
drop table list_bucketing_dynamic_part;

