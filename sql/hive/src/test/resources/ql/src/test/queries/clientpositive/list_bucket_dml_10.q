set hive.mapred.supports.subdirectories=true;
 
set mapred.input.dir.recursive=true;

-- run this test case in minimr to ensure it works in cluster

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
    skewed by (key) on ('484','51','103')
    stored as DIRECTORIES
    STORED AS RCFILE;

-- list bucketing DML without merge. use bucketize to generate a few small files.
explain extended
insert overwrite table list_bucketing_static_part partition (ds = '2008-04-08',  hr = '11')
select key, value from src;

insert overwrite table list_bucketing_static_part partition (ds = '2008-04-08', hr = '11')
select key, value from src;

-- check DML result
show partitions list_bucketing_static_part;
desc formatted list_bucketing_static_part partition (ds='2008-04-08', hr='11');	
