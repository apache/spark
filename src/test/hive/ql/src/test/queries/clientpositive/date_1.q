drop table date_1;

create table date_1 (d date);

insert overwrite table date_1 
  select cast('2011-01-01' as date) from src limit 1;

select * from date_1 limit 1;
select d, count(d) from date_1 group by d;

insert overwrite table date_1 
  select date '2011-01-01' from src limit 1;

select * from date_1 limit 1;
select d, count(d) from date_1 group by d;

insert overwrite table date_1 
  select cast(cast('2011-01-01 00:00:00' as timestamp) as date) from src limit 1;

select * from date_1 limit 1;
select d, count(d) from date_1 group by d;

-- Valid casts
select 
  cast('2012-01-01' as string), 
  cast(d as string), 
  cast(d as timestamp), 
  cast(cast(d as timestamp) as date), 
  cast(d as date)
from date_1 limit 1;

-- Invalid casts. 
select 
  cast(d as boolean), 
  cast(d as tinyint),
  cast(d as smallint),
  cast(d as int),
  cast(d as bigint),
  cast(d as float),
  cast(d as double)
from date_1 limit 1;

-- These comparisons should all be true
select 
  date '2011-01-01' = date '2011-01-01',
  unix_timestamp(date '2011-01-01') = unix_timestamp(date '2011-01-01'),
  unix_timestamp(date '2011-01-01') = unix_timestamp(cast(date '2011-01-01' as timestamp)),
  unix_timestamp(date '2011-01-01') = unix_timestamp(cast(cast('2011-01-01 12:13:14' as timestamp) as date)),
  unix_timestamp(date '2011-01-01') < unix_timestamp(cast('2011-01-01 00:00:01' as timestamp)),
  unix_timestamp(date '2011-01-01') = unix_timestamp(cast('2011-01-01 00:00:00' as timestamp)),
  unix_timestamp(date '2011-01-01') > unix_timestamp(cast('2010-12-31 23:59:59' as timestamp)),
  date '2011-01-01' = cast(timestamp('2011-01-01 23:24:25') as date),
  '2011-01-01' = cast(d as string),
  '2011-01-01' = cast(date '2011-01-01' as string)
from date_1 limit 1;

select 
  date('2001-01-28'),
  date('2001-02-28'),
  date('2001-03-28'),
  date('2001-04-28'),
  date('2001-05-28'),
  date('2001-06-28'),
  date('2001-07-28'),
  date('2001-08-28'),
  date('2001-09-28'),
  date('2001-10-28'),
  date('2001-11-28'),
  date('2001-12-28')
from date_1 limit 1;

select 
  unix_timestamp(date('2001-01-28')) = unix_timestamp(cast('2001-01-28 0:0:0' as timestamp)),
  unix_timestamp(date('2001-02-28')) = unix_timestamp(cast('2001-02-28 0:0:0' as timestamp)),
  unix_timestamp(date('2001-03-28')) = unix_timestamp(cast('2001-03-28 0:0:0' as timestamp)),
  unix_timestamp(date('2001-04-28')) = unix_timestamp(cast('2001-04-28 0:0:0' as timestamp)),
  unix_timestamp(date('2001-05-28')) = unix_timestamp(cast('2001-05-28 0:0:0' as timestamp)),
  unix_timestamp(date('2001-06-28')) = unix_timestamp(cast('2001-06-28 0:0:0' as timestamp)),
  unix_timestamp(date('2001-07-28')) = unix_timestamp(cast('2001-07-28 0:0:0' as timestamp)),
  unix_timestamp(date('2001-08-28')) = unix_timestamp(cast('2001-08-28 0:0:0' as timestamp)),
  unix_timestamp(date('2001-09-28')) = unix_timestamp(cast('2001-09-28 0:0:0' as timestamp)),
  unix_timestamp(date('2001-10-28')) = unix_timestamp(cast('2001-10-28 0:0:0' as timestamp)),
  unix_timestamp(date('2001-11-28')) = unix_timestamp(cast('2001-11-28 0:0:0' as timestamp)),
  unix_timestamp(date('2001-12-28')) = unix_timestamp(cast('2001-12-28 0:0:0' as timestamp))
from date_1 limit 1;

drop table date_1;
