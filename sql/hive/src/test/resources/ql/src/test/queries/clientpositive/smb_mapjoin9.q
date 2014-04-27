create table hive_test_smb_bucket1 (key int, value string) partitioned by (ds string) clustered by (key) sorted by (key) into 2 buckets;
create table hive_test_smb_bucket2 (key int, value string) partitioned by (ds string) clustered by (key) sorted by (key) into 2 buckets;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

-- empty partitions (HIVE-3205)
explain extended
SELECT /* + MAPJOIN(b) */ b.key as k1, b.value, b.ds, a.key as k2
FROM hive_test_smb_bucket1 a JOIN
hive_test_smb_bucket2 b
ON a.key = b.key WHERE a.ds = '2010-10-15' and b.ds='2010-10-15' and  b.key IS NOT NULL;

SELECT /* + MAPJOIN(b) */ b.key as k1, b.value, b.ds, a.key as k2
FROM hive_test_smb_bucket1 a JOIN
hive_test_smb_bucket2 b
ON a.key = b.key WHERE a.ds = '2010-10-15' and b.ds='2010-10-15' and  b.key IS NOT NULL;

explain extended
SELECT /* + MAPJOIN(a) */ b.key as k1, b.value, b.ds, a.key as k2
FROM hive_test_smb_bucket1 a JOIN
hive_test_smb_bucket2 b
ON a.key = b.key WHERE a.ds = '2010-10-15' and b.ds='2010-10-15' and  b.key IS NOT NULL;

SELECT /* + MAPJOIN(a) */ b.key as k1, b.value, b.ds, a.key as k2
FROM hive_test_smb_bucket1 a JOIN
hive_test_smb_bucket2 b
ON a.key = b.key WHERE a.ds = '2010-10-15' and b.ds='2010-10-15' and  b.key IS NOT NULL;

set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max = 1;

insert overwrite table hive_test_smb_bucket1 partition (ds='2010-10-15') select key, value from src;
insert overwrite table hive_test_smb_bucket2 partition (ds='2010-10-15') select key, value from src;

set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

explain
create table smb_mapjoin9_results as
SELECT /* + MAPJOIN(b) */ b.key as k1, b.value, b.ds, a.key as k2
FROM hive_test_smb_bucket1 a JOIN 
hive_test_smb_bucket2 b
ON a.key = b.key WHERE a.ds = '2010-10-15' and b.ds='2010-10-15' and  b.key IS NOT NULL;


create table smb_mapjoin9_results as
SELECT /* + MAPJOIN(b) */ b.key as k1, b.value, b.ds, a.key as k2
FROM hive_test_smb_bucket1 a JOIN 
hive_test_smb_bucket2 b
ON a.key = b.key WHERE a.ds = '2010-10-15' and b.ds='2010-10-15' and  b.key IS NOT NULL;

drop table smb_mapjoin9_results;
drop table hive_test_smb_bucket1;
drop table hive_test_smb_bucket2;