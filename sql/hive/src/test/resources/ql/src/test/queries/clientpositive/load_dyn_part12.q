show partitions srcpart;



create table if not exists nzhang_part12 like srcpart;
describe extended nzhang_part12;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.exec.dynamic.partition=true;


insert overwrite table nzhang_part12 partition (ds="2010-03-03", hr) select key, value, cast(hr*2 as int) from srcpart where ds is not null and hr is not null;

show partitions nzhang_part12;

select * from nzhang_part12 where ds is not null and hr is not null;


