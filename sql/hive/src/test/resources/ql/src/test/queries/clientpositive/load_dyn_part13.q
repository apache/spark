show partitions srcpart;



create table if not exists nzhang_part13 like srcpart;
describe extended nzhang_part13;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.exec.dynamic.partition=true;

explain
insert overwrite table nzhang_part13 partition (ds="2010-03-03", hr) 
select * from (
   select key, value, '22'
   from src
   where key < 20
   union all 
   select key, value, '33'
   from src 
   where key > 20 and key < 40) s;

insert overwrite table nzhang_part13 partition (ds="2010-03-03", hr) 
select * from (
   select key, value, '22'
   from src
   where key < 20
   union all 
   select key, value, '33'
   from src 
   where key > 20 and key < 40) s;

show partitions nzhang_part13;

select * from nzhang_part13 where ds is not null and hr is not null;


