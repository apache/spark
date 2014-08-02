set hive.mapred.supports.subdirectories=true;
set hive.optimize.listbucketing=true;
set mapred.input.dir.recursive=true;	
set hive.merge.mapfiles=false;	
set hive.merge.mapredfiles=false; 
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)	

-- List bucketing query logic test case. We simulate the directory structure by DML here. 
-- Test condition: 
-- 1. where clause has multiple skewed columns
-- 2. where clause doesn't have non-skewed column
-- 3. where clause has one and operator
-- Test focus:
-- 1. basic list bucketing query work
-- Test result:
-- 1. pruner only pick up right directory
-- 2. query result is right

-- create a skewed table
create table fact_daily (key String, value String) 
partitioned by (ds String, hr String) 
skewed by (key, value) on (('484','val_484'),('238','val_238')) 
stored as DIRECTORIES;

insert overwrite table fact_daily partition (ds = '1', hr = '4')
select key, value from src;

describe formatted fact_daily PARTITION (ds = '1', hr='4');
	
SELECT count(1) FROM fact_daily WHERE ds='1' and hr='4';	

-- pruner only pick up skewed-value directory
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended SELECT key FROM fact_daily WHERE ( ds='1' and hr='4') and (key='484' and value= 'val_484');
-- List Bucketing Query
SELECT key FROM fact_daily WHERE ( ds='1' and hr='4') and (key='484' and value= 'val_484');

-- pruner only pick up skewed-value directory
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended SELECT key,value FROM fact_daily WHERE ( ds='1' and hr='4') and (key='238' and value= 'val_238');
-- List Bucketing Query
SELECT key,value FROM fact_daily WHERE ( ds='1' and hr='4') and (key='238' and value= 'val_238');

-- pruner only pick up default directory
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended SELECT key FROM fact_daily WHERE ( ds='1' and hr='4') and (value = "3");
-- List Bucketing Query
SELECT key FROM fact_daily WHERE ( ds='1' and hr='4') and (value = "3");

-- pruner only pick up default directory
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended SELECT key,value FROM fact_daily WHERE ( ds='1' and hr='4') and key = '495';
-- List Bucketing Query
SELECT key,value FROM fact_daily WHERE ( ds='1' and hr='4') and key = '369';
