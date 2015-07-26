set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;


create table smb_bucket_input (key int, value string) stored as rcfile;
load data local inpath '../../data/files/smb_bucket_input.rc' into table smb_bucket_input;


CREATE TABLE smb_bucket4_1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS;

CREATE TABLE smb_bucket4_2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS;

CREATE TABLE smb_bucket4_3(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS;

insert overwrite table smb_bucket4_1 select * from smb_bucket_input where key=4 or key=2000 or key=4000;
insert overwrite table smb_bucket4_2 select * from smb_bucket_input where key=484 or key=3000 or key=5000;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

select /*+mapjoin(a)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;
select /*+mapjoin(b)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;


insert overwrite table smb_bucket4_1 select * from smb_bucket_input where key=2000 or key=4000;
insert overwrite table smb_bucket4_2 select * from smb_bucket_input where key=3000 or key=5000;

select /*+mapjoin(a)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;
select /*+mapjoin(b)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;


insert overwrite table smb_bucket4_1 select * from smb_bucket_input where key=4000;
insert overwrite table smb_bucket4_2 select * from smb_bucket_input where key=5000;

select /*+mapjoin(a)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;
select /*+mapjoin(b)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;


insert overwrite table smb_bucket4_1 select * from smb_bucket_input where key=1000 or key=4000;
insert overwrite table smb_bucket4_2 select * from smb_bucket_input where key=1000 or key=5000;

select /*+mapjoin(a)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;
select /*+mapjoin(b)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;


insert overwrite table smb_bucket4_1 select * from smb_bucket_input where key=1000 or key=4000;
insert overwrite table smb_bucket4_2 select * from smb_bucket_input where key=1000 or key=5000;
insert overwrite table smb_bucket4_3 select * from smb_bucket_input where key=1000 or key=5000;

select /*+mapjoin(b,c)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key
full outer join smb_bucket4_3 c on a.key=c.key;


insert overwrite table smb_bucket4_1 select * from smb_bucket_input where key=1000 or key=4000;
insert overwrite table smb_bucket4_2 select * from smb_bucket_input where key=1000 or key=5000;
insert overwrite table smb_bucket4_3 select * from smb_bucket_input where key=1000 or key=4000;

select /*+mapjoin(b,c)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key
full outer join smb_bucket4_3 c on a.key=c.key;


insert overwrite table smb_bucket4_1 select * from smb_bucket_input where key=4000;
insert overwrite table smb_bucket4_2 select * from smb_bucket_input where key=5000;
insert overwrite table smb_bucket4_3 select * from smb_bucket_input where key=4000;

select /*+mapjoin(b,c)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key
full outer join smb_bucket4_3 c on a.key=c.key;


insert overwrite table smb_bucket4_1 select * from smb_bucket_input where key=00000;
insert overwrite table smb_bucket4_2 select * from smb_bucket_input where key=4000;
insert overwrite table smb_bucket4_3 select * from smb_bucket_input where key=5000;

select /*+mapjoin(b,c)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key
full outer join smb_bucket4_3 c on a.key=c.key;


insert overwrite table smb_bucket4_1 select * from smb_bucket_input where key=1000;
insert overwrite table smb_bucket4_2 select * from smb_bucket_input where key=4000;
insert overwrite table smb_bucket4_3 select * from smb_bucket_input where key=5000;

select /*+mapjoin(b,c)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key
full outer join smb_bucket4_3 c on a.key=c.key;




