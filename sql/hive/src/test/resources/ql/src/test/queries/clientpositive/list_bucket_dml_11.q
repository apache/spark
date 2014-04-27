set hive.mapred.supports.subdirectories=true;
set mapred.input.dir.recursive=true;
set hive.merge.mapfiles=false;	
set hive.merge.mapredfiles=false;

-- Ensure it works if skewed column is not the first column in the table columns

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)

-- list bucketing DML: static partition. multiple skewed columns.

-- create a skewed table
create table list_bucketing_static_part (key String, value String) 
    partitioned by (ds String, hr String) 
    skewed by (value) on ('val_466','val_287','val_82')
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

set hive.optimize.listbucketing=true;
explain extended
select key, value from list_bucketing_static_part where ds='2008-04-08' and hr='11' and value = "val_466";
select key, value from list_bucketing_static_part where ds='2008-04-08' and hr='11' and value = "val_466";

drop table list_bucketing_static_part;
