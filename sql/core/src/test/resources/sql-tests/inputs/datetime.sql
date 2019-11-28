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

select date '2019-01-01\t';
select timestamp '2019-01-01\t';

create or replace temporary view tmp_dates as select * from values
 ('2011-11-11', '2011-11-11', '2011-11-11', '2011-11-11', 1),
 ('2011-11-10', '2011-11-11', '2011-11-11', '2011-11-12', 2),
 ('2011-11-11', '2011-11-10', '2011-11-11', '2011-11-12', 3),
 ('2011-11-11', '2011-11-10', '2011-11-12', '2011-11-11', 4),
 ('2011-11-10', '2011-11-11', '2011-11-12', '2011-11-13', 5),
 ('2011-11-10', '2011-11-20', '2011-11-11', '2011-11-19', 6),
 ('2011-11-11', '2011-11-19', '2011-11-10', '2011-11-20', 7),
 ('2011-11-11', '2011-11-19', '2011-11-10', null, 8) t(a, b, c, d, e);

select (cast(a as date), cast(b as date)) overlaps (cast(c as date), cast(d as date)), e from tmp_dates order by e;
select (cast(a as timestamp), cast(b as timestamp)) overlaps (cast(c as timestamp), cast(d as timestamp)), e from tmp_dates order by e;
select (cast(a as timestamp), cast(b as date)) overlaps (cast(c as date), cast(d as timestamp)), e from tmp_dates order by e;
select e from tmp_dates where (cast(a as timestamp), cast(b as date)) overlaps (cast(c as date), cast(d as timestamp)) order by e;
