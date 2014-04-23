drop table partition_varchar_1;

create table partition_varchar_1 (key string, value varchar(20)) partitioned by (dt varchar(10), region int);

insert overwrite table partition_varchar_1 partition(dt='2000-01-01', region=1)
  select * from src limit 10;
insert overwrite table partition_varchar_1 partition(dt='2000-01-01', region=2)
  select * from src limit 5;
insert overwrite table partition_varchar_1 partition(dt='2013-08-08', region=1)
  select * from src limit 20;
insert overwrite table partition_varchar_1 partition(dt='2013-08-08', region=10)
  select * from src limit 11;

select distinct dt from partition_varchar_1;
select * from partition_varchar_1 where dt = '2000-01-01' and region = 2 order by key,value;

-- 15
select count(*) from partition_varchar_1 where dt = '2000-01-01';
-- 5
select count(*) from partition_varchar_1 where dt = '2000-01-01' and region = 2;
-- 11
select count(*) from partition_varchar_1 where dt = '2013-08-08' and region = 10;
-- 30
select count(*) from partition_varchar_1 where region = 1;
-- 0
select count(*) from partition_varchar_1 where dt = '2000-01-01' and region = 3;
-- 0
select count(*) from partition_varchar_1 where dt = '1999-01-01';

-- Try other comparison operations

-- 20
select count(*) from partition_varchar_1 where dt > '2000-01-01' and region = 1;
-- 10
select count(*) from partition_varchar_1 where dt < '2000-01-02' and region = 1;
-- 20
select count(*) from partition_varchar_1 where dt >= '2000-01-02' and region = 1;
-- 10
select count(*) from partition_varchar_1 where dt <= '2000-01-01' and region = 1;
-- 20
select count(*) from partition_varchar_1 where dt <> '2000-01-01' and region = 1;

drop table partition_varchar_1;
