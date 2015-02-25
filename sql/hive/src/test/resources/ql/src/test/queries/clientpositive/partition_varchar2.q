drop table partition_varchar_2;

create table partition_varchar_2 (key string, value varchar(20)) partitioned by (dt varchar(15), region int);

insert overwrite table partition_varchar_2 partition(dt='2000-01-01', region=1)
  select * from src order by key limit 1;

select * from partition_varchar_2 where cast(dt as varchar(10)) = '2000-01-01';

drop table partition_varchar_2;
