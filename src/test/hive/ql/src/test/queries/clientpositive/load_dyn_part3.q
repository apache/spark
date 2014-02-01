show partitions srcpart;



create table if not exists nzhang_part3 like srcpart;
describe extended nzhang_part3;

set hive.merge.mapfiles=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

explain
insert overwrite table nzhang_part3 partition (ds, hr) select key, value, ds, hr from srcpart where ds is not null and hr is not null;

insert overwrite table nzhang_part3 partition (ds, hr) select key, value, ds, hr from srcpart where ds is not null and hr is not null;

select * from nzhang_part3 where ds is not null and hr is not null;


