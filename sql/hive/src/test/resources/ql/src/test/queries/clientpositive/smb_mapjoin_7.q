set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;


CREATE TABLE smb_bucket4_1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;


CREATE TABLE smb_bucket4_2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;




create table smb_join_results(k1 int, v1 string, k2 int, v2 string);
create table smb_join_results_empty_bigtable(k1 int, v1 string, k2 int, v2 string);
create table normal_join_results(k1 int, v1 string, k2 int, v2 string);

load data local inpath '../../data/files/empty1.txt' into table smb_bucket4_1;
load data local inpath '../../data/files/empty2.txt' into table smb_bucket4_1;

insert overwrite table smb_bucket4_2
select * from src;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

insert overwrite table smb_join_results_empty_bigtable
select /*+mapjoin(b)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;

insert overwrite table smb_join_results_empty_bigtable
select /*+mapjoin(b)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;

select * from smb_join_results_empty_bigtable order by k1, v1, k2, v2;

explain
insert overwrite table smb_join_results
select /*+mapjoin(a)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;

insert overwrite table smb_join_results
select /*+mapjoin(a)*/ * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;

select * from smb_join_results order by k1, v1, k2, v2;

insert overwrite table normal_join_results select * from smb_bucket4_1 a full outer join smb_bucket4_2 b on a.key = b.key;

select sum(hash(k1)) as k1, sum(hash(k2)) as k2, sum(hash(v1)) as v1, sum(hash(v2)) as v2 from normal_join_results;
select sum(hash(k1)) as k1, sum(hash(k2)) as k2, sum(hash(v1)) as v1, sum(hash(v2)) as v2 from smb_join_results;
select sum(hash(k1)) as k1, sum(hash(k2)) as k2, sum(hash(v1)) as v1, sum(hash(v2)) as v2 from smb_join_results_empty_bigtable;






