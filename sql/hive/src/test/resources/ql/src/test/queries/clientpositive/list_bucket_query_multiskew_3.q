set hive.mapred.supports.subdirectories=true;
set hive.optimize.listbucketing=true;
set mapred.input.dir.recursive=true;	
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)	

-- List bucketing query logic test case. We simulate the directory structure by DML here.
-- Test condition: 
-- 1. where clause has multiple skewed columns and non-skewed columns
-- 3. where clause has a few operators
-- Test focus:
-- 1. query works for on partition level. 
--    A table can mix up non-skewed partition and skewed partition
--    Even for skewed partition, it can have different skewed information.
-- Test result:
-- 1. pruner only pick up right directory
-- 2. query result is right

-- create a skewed table
create table fact_daily (key String, value String) 
partitioned by (ds String, hr String) ;

-- partition no skew
insert overwrite table fact_daily partition (ds = '1', hr = '1')
select key, value from src;
describe formatted fact_daily PARTITION (ds = '1', hr='1');

-- partition. skewed value is 484/238
alter table fact_daily skewed by (key, value) on (('484','val_484'),('238','val_238')) stored as DIRECTORIES;
insert overwrite table fact_daily partition (ds = '1', hr = '2')
select key, value from src;
describe formatted fact_daily PARTITION (ds = '1', hr='2');

-- another partition. skewed value is 327
alter table fact_daily skewed by (key, value) on (('327','val_327')) stored as DIRECTORIES;
insert overwrite table fact_daily partition (ds = '1', hr = '3')
select key, value from src;
describe formatted fact_daily PARTITION (ds = '1', hr='3');

-- query non-skewed partition
explain extended
select * from fact_daily where ds = '1' and  hr='1' and key='145';
select * from fact_daily where ds = '1' and  hr='1' and key='145';
explain extended
select count(*) from fact_daily where ds = '1' and  hr='1';
select count(*) from fact_daily where ds = '1' and  hr='1';
	
-- query skewed partition
explain extended
SELECT * FROM fact_daily WHERE ds='1' and hr='2' and (key='484' and value='val_484');	
SELECT * FROM fact_daily WHERE ds='1' and hr='2' and (key='484' and value='val_484');

-- query another skewed partition
explain extended
SELECT * FROM fact_daily WHERE ds='1' and hr='3' and (key='327' and value='val_327');	
SELECT * FROM fact_daily WHERE ds='1' and hr='3' and (key='327' and value='val_327');	
