set hive.mapred.supports.subdirectories=true;
set hive.optimize.listbucketing=true;
set mapred.input.dir.recursive=true;	
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;	

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)	

-- List bucketing query logic test case. 
-- Test condition: 
-- 1. where clause has only one skewed column
-- 2. where clause doesn't have non-skewed column
-- Test focus:
-- 1. list bucketing query logic works fine for subquery
-- Test result:
-- 1. pruner only pick up right directory
-- 2. query result is right

-- create 2 tables: fact_daily and fact_tz
-- fact_daily will be used for list bucketing query
-- fact_tz is a table used to prepare data and test directories	
CREATE TABLE fact_daily(x int, y STRING) PARTITIONED BY (ds STRING);	
CREATE TABLE fact_tz(x int, y STRING) PARTITIONED BY (ds STRING, hr STRING)	
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/fact_tz';	

-- create /fact_tz/ds=1/hr=1 directory	
INSERT OVERWRITE TABLE fact_tz PARTITION (ds='1', hr='1')	
SELECT key, value FROM src WHERE key=484;	

-- create /fact_tz/ds=1/hr=2 directory	
INSERT OVERWRITE TABLE fact_tz PARTITION (ds='1', hr='2')	
SELECT key+11, value FROM src WHERE key=484;

dfs -lsr ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/hr=1 ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/x=484;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/hr=2 ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME;
dfs -lsr ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1;

-- switch fact_daily to skewed table and point its location to /fact_tz/ds=1
alter table fact_daily skewed by (x) on (484);
ALTER TABLE fact_daily SET TBLPROPERTIES('EXTERNAL'='TRUE');	
ALTER TABLE fact_daily ADD PARTITION (ds='1')	
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1';	

-- set List Bucketing location map
alter table fact_daily PARTITION (ds = '1') set skewed location (484='${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/x=484','HIVE_DEFAULT_LIST_BUCKETING_KEY'='${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME');
describe formatted fact_daily PARTITION (ds = '1');
	
SELECT * FROM fact_daily WHERE ds='1' ORDER BY x, y;

-- The first subquery
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended select x from (select x from fact_daily where ds = '1') subq where x = 484;
-- List Bucketing Query
select x from (select * from fact_daily where ds = '1') subq where x = 484;

-- The second subquery
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended select x1, y1 from(select x as x1, y as y1 from fact_daily where ds ='1') subq where x1 = 484 ORDER BY x1, y1;
-- List Bucketing Query
select x1, y1 from(select x as x1, y as y1 from fact_daily where ds ='1') subq where x1 = 484 ORDER BY x1, y1;


-- The third subquery
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended  select y, count(1) from fact_daily where ds ='1' and x = 484 group by y;
-- List Bucketing Query
select y, count(1) from fact_daily where ds ='1' and x = 484 group by y;

-- The fourth subquery
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended  select x, c from (select x, count(1) as c from fact_daily where ds = '1' group by x) subq where x = 484;;
-- List Bucketing Query
select x, c from (select x, count(1) as c from fact_daily where ds = '1' group by x) subq where x = 484;
