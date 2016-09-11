set hive.optimize.listbucketing=true;
set mapred.input.dir.recursive=true;	
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.mapred.supports.subdirectories=true;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)	

-- List bucketing query logic test case. 
-- Test condition: 
-- 1. where clause has single skewed columns and non-skewed columns
-- 3. where clause has a few operators
-- Test focus:
-- 1. basic list bucketing query works for not (equal) case
-- Test result:
-- 1. pruner only pick up right directory
-- 2. query result is right

-- create 2 tables: fact_daily and fact_tz
-- fact_daily will be used for list bucketing query
-- fact_tz is a table used to prepare data and test directories	
CREATE TABLE fact_daily(x int, y STRING, z STRING) PARTITIONED BY (ds STRING);	
CREATE TABLE fact_tz(x int, y STRING, z STRING) PARTITIONED BY (ds STRING, hr STRING)	
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/fact_tz';	

-- create /fact_tz/ds=1/hr=1 directory	
INSERT OVERWRITE TABLE fact_tz PARTITION (ds='1', hr='1')	
SELECT key, value, value FROM src WHERE key=484;	

-- create /fact_tz/ds=1/hr=2 directory	
INSERT OVERWRITE TABLE fact_tz PARTITION (ds='1', hr='2')	
SELECT key, value, value FROM src WHERE key=278 or key=86;

-- create /fact_tz/ds=1/hr=3 directory	
INSERT OVERWRITE TABLE fact_tz PARTITION (ds='1', hr='3')	
SELECT key, value, value FROM src WHERE key=238;

dfs -lsr ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/hr=1 ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/x=484;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/hr=2 ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/hr=3 ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/x=238;
dfs -lsr ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1;	

-- switch fact_daily to skewed table and point its location to /fact_tz/ds=1
alter table fact_daily skewed by (x) on (484,238);
ALTER TABLE fact_daily SET TBLPROPERTIES('EXTERNAL'='TRUE');	
ALTER TABLE fact_daily ADD PARTITION (ds='1')	
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1';	

-- set List Bucketing location map
alter table fact_daily PARTITION (ds = '1') set skewed location (484='${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/x=484',
238='${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/x=238',
'HIVE_DEFAULT_LIST_BUCKETING_KEY'='${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME');
describe formatted fact_daily PARTITION (ds = '1');
	
SELECT * FROM fact_daily WHERE ds='1' ORDER BY x, y, z;	

-- pruner  pick up right directory
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended SELECT x FROM fact_daily WHERE ds='1' and not (x = 86) ORDER BY x;
-- List Bucketing Query
SELECT x FROM fact_daily WHERE ds='1' and not (x = 86) ORDER BY x;
