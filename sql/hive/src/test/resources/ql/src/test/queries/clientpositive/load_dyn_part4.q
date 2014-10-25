show partitions srcpart;



create table if not exists nzhang_part4 like srcpart;
describe extended nzhang_part4;

set hive.merge.mapfiles=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table nzhang_part4 partition (ds='2008-04-08', hr='existing_value') select key, value from src;

explain
insert overwrite table nzhang_part4 partition (ds, hr) select key, value, ds, hr from srcpart where ds is not null and hr is not null;

insert overwrite table nzhang_part4 partition (ds, hr) select key, value, ds, hr from srcpart where ds is not null and hr is not null;

show partitions nzhang_part4;
select * from nzhang_part4 where ds='2008-04-08' and hr is not null order by ds, hr, key;

select * from nzhang_part4 where ds is not null and hr is not null order by ds, hr, key;


