-- INCLUDE_OS_WINDOWS
-- included only on  windows because of difference in file name encoding logic


create table if not exists nzhang_part14 (key string) 
  partitioned by (value string);

describe extended nzhang_part14;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

explain
insert overwrite table nzhang_part14 partition(value) 
select key, value from (
  select 'k1' as key, cast(null as string) as value from src limit 2
  union all
  select 'k2' as key, '' as value from src limit 2
  union all 
  select 'k3' as key, ' ' as value from src limit 2
) T;

insert overwrite table nzhang_part14 partition(value) 
select key, value from (
  select 'k1' as key, cast(null as string) as value from src limit 2
  union all
  select 'k2' as key, '' as value from src limit 2
  union all 
  select 'k3' as key, ' ' as value from src limit 2
) T;


show partitions nzhang_part14;

select * from nzhang_part14 where value <> 'a'
order by key, value;


