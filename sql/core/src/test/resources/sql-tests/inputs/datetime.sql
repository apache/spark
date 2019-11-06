-- date time functions

-- [SPARK-16836] current_date and current_timestamp literals
select current_date = current_date(), current_timestamp = current_timestamp();

select to_date(null), to_date('2016-12-31'), to_date('2016-12-31', 'yyyy-MM-dd');

select to_timestamp(null), to_timestamp('2016-12-31 00:12:00'), to_timestamp('2016-12-31', 'yyyy-MM-dd');

select dayofweek('2007-02-03'), dayofweek('2009-07-30'), dayofweek('2017-05-27'), dayofweek(null), dayofweek('1582-10-15 13:10:15');

-- [SPARK-22333]: timeFunctionCall has conflicts with columnReference
create temporary view ttf1 as select * from values
  (1, 2),
  (2, 3)
  as ttf1(current_date, current_timestamp);
  
select current_date, current_timestamp from ttf1;

create temporary view ttf2 as select * from values
  (1, 2),
  (2, 3)
  as ttf2(a, b);
  
select current_date = current_date(), current_timestamp = current_timestamp(), a, b from ttf2;

select a, b from ttf2 order by a, current_date;

select weekday('2007-02-03'), weekday('2009-07-30'), weekday('2017-05-27'), weekday(null), weekday('1582-10-15 13:10:15');

select year('1500-01-01'), month('1500-01-01'), dayOfYear('1500-01-01');

select date '2001-09-28' + 7;
select 7 + date '2001-09-28';
select date '2001-10-01' - 7;
select date '2001-10-01' - date '2001-09-28';
select date'2020-01-01' - timestamp'2019-10-06 10:11:12.345678';
select timestamp'2019-10-06 10:11:12.345678' - date'2020-01-01';

-- interval operations
select 3 * (timestamp'2019-10-15 10:11:12.001002' - date'2019-10-15');
select interval 4 month 2 weeks 3 microseconds * 1.5;
select (timestamp'2019-10-15' - timestamp'2019-10-14') / 1.5;

-- interval operation with null and zero case
select interval '1 year 2 month' / null;
select interval '1 year 2 month' / 0;
select interval '4 months 2 weeks 6 days' * null;
select null * interval '4 months 2 weeks 6 days';

select
  k,
  v,
  cast(k as interval) / v,
  cast(k as interval) * v
from VALUES
  ('1 seconds', 1),
  ('2 seconds', 0),
  ('3 seconds', null),
  (null, null),
  (null, 0) t(k, v);
