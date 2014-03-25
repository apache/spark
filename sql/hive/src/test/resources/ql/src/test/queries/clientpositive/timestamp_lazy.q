drop table timestamp_lazy;
create table timestamp_lazy (t timestamp, key string, value string);
insert overwrite table timestamp_lazy select cast('2011-01-01 01:01:01' as timestamp), key, value from src limit 5;

select t,key,value from timestamp_lazy ORDER BY key ASC, value ASC;
select t,key,value from timestamp_lazy distribute by t sort by key ASC, value ASC;
