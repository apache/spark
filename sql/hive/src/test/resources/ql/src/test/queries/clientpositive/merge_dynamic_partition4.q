-- this test verifies that the block merge task that can follow a query to generate dynamic
-- partitions does not produce incorrect results by dropping partitions

create table srcpart_merge_dp like srcpart;

create table srcpart_merge_dp_rc like srcpart;
alter table srcpart_merge_dp_rc set fileformat RCFILE;

create table merge_dynamic_part like srcpart;
alter table merge_dynamic_part set fileformat RCFILE;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=12);
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=12);
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=12);
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=12);

insert overwrite table srcpart_merge_dp_rc partition (ds = '2008-04-08', hr) 
select key, value, hr from srcpart_merge_dp where ds = '2008-04-08';

set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=10000000000000;
set hive.exec.compress.output=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

explain
insert overwrite table merge_dynamic_part partition (ds = '2008-04-08', hr)
select key, value, if(key % 2 == 0, 'a1', 'b1') as hr from srcpart_merge_dp_rc where ds = '2008-04-08';

insert overwrite table merge_dynamic_part partition (ds = '2008-04-08', hr)
select key, value, if(key % 2 == 0, 'a1', 'b1') as hr from srcpart_merge_dp_rc where ds = '2008-04-08';

show partitions merge_dynamic_part;

select count(*) from merge_dynamic_part;
