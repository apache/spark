set hive.mapred.supports.subdirectories=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.merge.smallfiles.avgsize=200;
set mapred.input.dir.recursive=true;
set hive.merge.mapfiles=false;	
set hive.merge.mapredfiles=false;
set hive.stats.reliable=true;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)

-- list bucketing DML: static partition. multiple skewed columns.
-- ds=2008-04-08/hr=11/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME:
--  5263 000000_0
--  5263 000001_0
-- ds=2008-04-08/hr=11/key=103/value=val_103:
-- 99 000000_0
-- 99 000001_0
-- ds=2008-04-08/hr=11/key=484/value=val_484:
-- 87 000000_0
-- 87 000001_0

-- create a skewed table
create table list_bucketing_static_part (key String, value String) 
    partitioned by (ds String, hr String) 
    skewed by (key, value) on (('484','val_484'),('51','val_14'),('103','val_103'))
    stored as DIRECTORIES
    STORED AS RCFILE;

-- list bucketing DML without merge. use bucketize to generate a few small files.
explain extended
insert overwrite table list_bucketing_static_part partition (ds = '2008-04-08',  hr = '11')
select key, value from srcpart where ds = '2008-04-08';

insert overwrite table list_bucketing_static_part partition (ds = '2008-04-08', hr = '11')
select key, value from srcpart where ds = '2008-04-08';

-- check DML result
show partitions list_bucketing_static_part;
desc formatted list_bucketing_static_part partition (ds='2008-04-08', hr='11');	

select count(1) from srcpart where ds = '2008-04-08';
select count(*) from list_bucketing_static_part;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.optimize.listbucketing=true;
explain extended
select * from list_bucketing_static_part where ds = '2008-04-08' and  hr = '11' and key = '484' and value = 'val_484' ORDER BY key, value, ds, hr;
select * from list_bucketing_static_part where ds = '2008-04-08' and  hr = '11' and key = '484' and value = 'val_484' ORDER BY key, value, ds, hr;
select * from srcpart where ds = '2008-04-08' and key = '484' and value = 'val_484' ORDER BY key, value;

-- 51 and val_51 in the table so skewed data for 51 and val_14 should be none
-- but query should succeed for 51 or 51 and val_14
select * from srcpart where ds = '2008-04-08' and key = '51' ORDER BY key, value;
select * from list_bucketing_static_part where key = '51' ORDER BY key, value, ds, hr;
select * from srcpart where ds = '2008-04-08' and key = '51' and value = 'val_14'  ORDER BY key, value;
select * from list_bucketing_static_part where key = '51' and value = 'val_14' ORDER BY key, value, ds, hr;

-- queries with < <= > >= should work for skewed test although we don't benefit from pruning
select count(1) from srcpart where ds = '2008-04-08' and key < '51';
select count(1) from list_bucketing_static_part where key < '51';
select count(1) from srcpart where ds = '2008-04-08' and key <= '51';
select count(1) from list_bucketing_static_part where key <= '51';
select count(1) from srcpart where ds = '2008-04-08' and key > '51';
select count(1) from list_bucketing_static_part where key > '51';
select count(1) from srcpart where ds = '2008-04-08' and key >= '51';
select count(1) from list_bucketing_static_part where key >= '51';

-- clean up
drop table list_bucketing_static_part;
