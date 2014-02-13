show partitions srcpart;


create table if not exists nzhang_part like srcpart;
describe extended nzhang_part;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.exec.compress.output=true;
set hive.exec.dynamic.partition=true;

insert overwrite table nzhang_part partition (ds="2010-03-03", hr) select key, value, hr from srcpart where ds is not null and hr is not null;

select * from nzhang_part where ds = '2010-03-03' and hr = '11';
select * from nzhang_part where ds = '2010-03-03' and hr = '12';


