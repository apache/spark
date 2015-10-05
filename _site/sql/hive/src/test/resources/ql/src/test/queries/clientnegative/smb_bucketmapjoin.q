set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;


CREATE TABLE smb_bucket4_1(key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS;


CREATE TABLE smb_bucket4_2(key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS;

insert overwrite table smb_bucket4_1
select * from src;

insert overwrite table smb_bucket4_2
select * from src;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;

select /*+mapjoin(a)*/ * from smb_bucket4_1 a left outer join smb_bucket4_2 b on a.key = b.key;



