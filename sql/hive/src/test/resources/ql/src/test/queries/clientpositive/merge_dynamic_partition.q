set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table srcpart_merge_dp like srcpart;

create table merge_dynamic_part like srcpart;

load data local inpath '../data/files/srcbucket20.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
load data local inpath '../data/files/srcbucket21.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
load data local inpath '../data/files/srcbucket22.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);
load data local inpath '../data/files/srcbucket23.txt' INTO TABLE srcpart_merge_dp partition(ds='2008-04-08', hr=11);

set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat; 
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.merge.smallfiles.avgsize=1000000000;
explain
insert overwrite table merge_dynamic_part partition (ds='2008-04-08', hr) select key, value, hr from srcpart_merge_dp where ds='2008-04-08';
insert overwrite table merge_dynamic_part partition (ds='2008-04-08', hr) select key, value, hr from srcpart_merge_dp where ds='2008-04-08';

select * from merge_dynamic_part order by key, value;
show table extended like `merge_dynamic_part`;


set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=1000000000;
explain
insert overwrite table merge_dynamic_part partition (ds='2008-04-08', hr=11) select key, value from srcpart_merge_dp where ds='2008-04-08';
insert overwrite table merge_dynamic_part partition (ds='2008-04-08', hr=11) select key, value from srcpart_merge_dp where ds='2008-04-08';

select * from merge_dynamic_part order by key, value;
show table extended like `merge_dynamic_part`;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=1000000000;
explain
insert overwrite table merge_dynamic_part partition (ds, hr) select key, value, ds, hr from srcpart_merge_dp where ds='2008-04-08' and hr=11;
insert overwrite table merge_dynamic_part partition (ds, hr) select key, value, ds, hr from srcpart_merge_dp where ds='2008-04-08' and hr=11;;

select * from merge_dynamic_part order by key, value;
show table extended like `merge_dynamic_part`;

