

create table if not exists nzhang_part5 (key string) partitioned by (value string);
describe extended nzhang_part5;

set hive.merge.mapfiles=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=2000;

explain
insert overwrite table nzhang_part5 partition (value) select key, value from src;

insert overwrite table nzhang_part5 partition (value) select key, value from src;

show partitions nzhang_part5;

select * from nzhang_part5 where value='val_0';
select * from nzhang_part5 where value='val_2';


