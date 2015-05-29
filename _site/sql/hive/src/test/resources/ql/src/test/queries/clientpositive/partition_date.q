drop table partition_date_1;

create table partition_date_1 (key string, value string) partitioned by (dt date, region string);

insert overwrite table partition_date_1 partition(dt='2000-01-01', region= '1')
  select * from src tablesample (10 rows);
insert overwrite table partition_date_1 partition(dt='2000-01-01', region= '2')
  select * from src tablesample (5 rows);
insert overwrite table partition_date_1 partition(dt='2013-12-10', region= '2020-20-20')
  select * from src tablesample (5 rows);
insert overwrite table partition_date_1 partition(dt='2013-08-08', region= '1') 
  select * from src tablesample (20 rows);
insert overwrite table partition_date_1 partition(dt='2013-08-08', region= '10') 
  select * from src tablesample (11 rows);


select distinct dt from partition_date_1;
select * from partition_date_1 where dt = '2000-01-01' and region = '2' order by key,value;

-- 15
select count(*) from partition_date_1 where dt = date '2000-01-01';
-- 15.  Also try with string value in predicate
select count(*) from partition_date_1 where dt = '2000-01-01';
-- 5
select count(*) from partition_date_1 where dt = date '2000-01-01' and region = '2';
-- 11
select count(*) from partition_date_1 where dt = date '2013-08-08' and region = '10';
-- 30
select count(*) from partition_date_1 where region = '1';
-- 0
select count(*) from partition_date_1 where dt = date '2000-01-01' and region = '3';
-- 0
select count(*) from partition_date_1 where dt = date '1999-01-01';

-- Try other comparison operations

-- 20
select count(*) from partition_date_1 where dt > date '2000-01-01' and region = '1';
-- 10
select count(*) from partition_date_1 where dt < date '2000-01-02' and region = '1';
-- 20
select count(*) from partition_date_1 where dt >= date '2000-01-02' and region = '1';
-- 10
select count(*) from partition_date_1 where dt <= date '2000-01-01' and region = '1';
-- 20
select count(*) from partition_date_1 where dt <> date '2000-01-01' and region = '1';
-- 10
select count(*) from partition_date_1 where dt between date '1999-12-30' and date '2000-01-03' and region = '1';


-- Try a string key with date-like strings

-- 5
select count(*) from partition_date_1 where region = '2020-20-20';
-- 5
select count(*) from partition_date_1 where region > '2010-01-01';

drop table partition_date_1;
