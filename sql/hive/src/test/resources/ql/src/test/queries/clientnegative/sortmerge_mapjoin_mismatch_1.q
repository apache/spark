create table table_asc(key int, value string) CLUSTERED BY (key) SORTED BY (key asc) 
INTO 1 BUCKETS STORED AS RCFILE; 
create table table_desc(key int, value string) CLUSTERED BY (key) SORTED BY (key desc) 
INTO 1 BUCKETS STORED AS RCFILE;

set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;

insert overwrite table table_asc select key, value from src; 
insert overwrite table table_desc select key, value from src;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

-- If the user asked for sort merge join to be enforced (by setting
-- hive.enforce.sortmergebucketmapjoin to true), an error should be thrown, since
-- one of the tables is in ascending order and the other is in descending order,
-- and sort merge bucket mapjoin cannot be performed. In the default mode, the
-- query would succeed, although a regular map-join would be performed instead of
-- what the user asked.

explain 
select /*+mapjoin(a)*/ * from table_asc a join table_desc b on a.key = b.key;

set hive.enforce.sortmergebucketmapjoin=true;

explain 
select /*+mapjoin(a)*/ * from table_asc a join table_desc b on a.key = b.key;
