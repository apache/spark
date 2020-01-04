-- test for intervals

-- greater than or equal
select interval '1 day' > interval '23 hour';
select interval '-1 day' >= interval '-23 hour';
select interval '-1 day' > null;
select null > interval '-1 day';

-- less than or equal
select interval '1 minutes' < interval '1 hour';
select interval '-1 day' <= interval '-23 hour';

-- equal
select interval '1 year' = interval '360 days';
select interval '1 year 2 month' = interval '420 days';
select interval '1 year' = interval '365 days';
select interval '1 month' = interval '30 days';
select interval '1 minutes' = interval '1 hour';
select interval '1 minutes' = null;
select null = interval '-1 day';

-- null safe equal
select interval '1 minutes' <=> null;
select null <=> interval '1 minutes';

-- complex interval representation
select INTERVAL '9 years 1 months -1 weeks -4 days -10 hours -46 minutes' > interval '1 minutes';

-- ordering
select cast(v as interval) i from VALUES ('1 seconds'), ('4 seconds'), ('3 seconds') t(v) order by i;

-- unlimited days
select interval '1 month 120 days' > interval '2 month';
select interval '1 month 30 days' = interval '2 month';

-- unlimited microseconds
select interval '1 month 29 days 40 hours' > interval '2 month';

-- max
select max(cast(v as interval)) from VALUES ('1 seconds'), ('4 seconds'), ('3 seconds') t(v);

-- min
select min(cast(v as interval)) from VALUES ('1 seconds'), ('4 seconds'), ('3 seconds') t(v);

-- multiply and divide an interval by a number
select 3 * (timestamp'2019-10-15 10:11:12.001002' - date'2019-10-15');
select interval 4 month 2 weeks 3 microseconds * 1.5;
select (timestamp'2019-10-15' - timestamp'2019-10-14') / 1.5;

-- interval operation with null and zero case
select interval '2 seconds' / 0;
select interval '2 seconds' / null;
select interval '2 seconds' * null;
select null * interval '2 seconds';

-- interval with a positive/negative sign
select -interval '-1 month 1 day -1 second';
select -interval -1 month 1 day -1 second;
select +interval '-1 month 1 day -1 second';
select +interval -1 month 1 day -1 second;

-- make intervals
select make_interval(1);
select make_interval(1, 2);
select make_interval(1, 2, 3);
select make_interval(1, 2, 3, 4);
select make_interval(1, 2, 3, 4, 5);
select make_interval(1, 2, 3, 4, 5, 6);
select make_interval(1, 2, 3, 4, 5, 6, 7.008009);

-- cast string to intervals
select cast('1 second' as interval);
select cast('+1 second' as interval);
select cast('-1 second' as interval);
select cast('+     1 second' as interval);
select cast('-     1 second' as interval);
select cast('- -1 second' as interval);
select cast('- +1 second' as interval);

-- interval literal
select interval 13.123456789 seconds, interval -13.123456789 second;
select interval 1 year 2 month 3 week 4 day 5 hour 6 minute 7 seconds 8 millisecond 9 microsecond;
select interval '30' year '25' month '-100' day '40' hour '80' minute '299.889987299' second;
select interval '0 0:0:0.1' day to second;
select interval '10-9' year to month;
select interval '20 15' day to hour;
select interval '20 15:40' day to minute;
select interval '20 15:40:32.99899999' day to second;
select interval '15:40' hour to minute;
select interval '15:40:32.99899999' hour to second;
select interval '40:32.99899999' minute to second;
select interval '40:32' minute to second;
select interval 30 day day;

-- invalid day-time string intervals
select interval '20 15:40:32.99899999' day to hour;
select interval '20 15:40:32.99899999' day to minute;
select interval '15:40:32.99899999' hour to minute;
select interval '15:40.99899999' hour to second;
select interval '15:40' hour to second;
select interval '20 40:32.99899999' minute to second;

-- ns is not supported
select interval 10 nanoseconds;

-- map + interval test
select map(1, interval 1 day, 2, interval 3 week);

-- typed interval expression
select interval 'interval 3 year 1 hour';
select interval '3 year 1 hour';

-- malformed interval literal
select interval;
select interval 1 fake_unit;
select interval 1 year to month;
select interval '1' year to second;
select interval '10-9' year to month '2-1' year to month;
select interval '10-9' year to month '12:11:10' hour to second;
select interval '1 15:11' day to minute '12:11:10' hour to second;
select interval 1 year '2-1' year to month;
select interval 1 year '12:11:10' hour to second;
select interval '10-9' year to month '1' year;
select interval '12:11:10' hour to second '1' year;
select interval (-30) day;
select interval (a + 1) day;
select interval 30 day day day;

-- sum interval values
-- null
select sum(cast(null as interval));

-- empty set
select sum(cast(v as interval)) from VALUES ('1 seconds') t(v) where 1=0;

-- basic interval sum
select sum(cast(v as interval)) from VALUES ('1 seconds'), ('2 seconds'), (null) t(v);
select sum(cast(v as interval)) from VALUES ('-1 seconds'), ('2 seconds'), (null) t(v);
select sum(cast(v as interval)) from VALUES ('-1 seconds'), ('-2 seconds'), (null) t(v);
select sum(cast(v as interval)) from VALUES ('-1 weeks'), ('2 seconds'), (null) t(v);

-- group by
select
    i,
    sum(cast(v as interval))
from VALUES (1, '-1 weeks'), (2, '2 seconds'), (3, null), (1, '5 days') t(i, v)
group by i;

-- having
select
    sum(cast(v as interval)) as sv
from VALUES (1, '-1 weeks'), (2, '2 seconds'), (3, null), (1, '5 days') t(i, v)
having sv is not null;

-- window
SELECT
    i,
    sum(cast(v as interval)) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
FROM VALUES(1, '1 seconds'), (1, '2 seconds'), (2, NULL), (2, NULL) t(i,v);

-- average with interval type
-- null
select avg(cast(v as interval)) from VALUES (null) t(v);

-- empty set
select avg(cast(v as interval)) from VALUES ('1 seconds'), ('2 seconds'), (null) t(v) where 1=0;

-- basic interval avg
select avg(cast(v as interval)) from VALUES ('1 seconds'), ('2 seconds'), (null) t(v);
select avg(cast(v as interval)) from VALUES ('-1 seconds'), ('2 seconds'), (null) t(v);
select avg(cast(v as interval)) from VALUES ('-1 seconds'), ('-2 seconds'), (null) t(v);
select avg(cast(v as interval)) from VALUES ('-1 weeks'), ('2 seconds'), (null) t(v);

-- group by
select
    i,
    avg(cast(v as interval))
from VALUES (1, '-1 weeks'), (2, '2 seconds'), (3, null), (1, '5 days') t(i, v)
group by i;

-- having
select
    avg(cast(v as interval)) as sv
from VALUES (1, '-1 weeks'), (2, '2 seconds'), (3, null), (1, '5 days') t(i, v)
having sv is not null;

-- window
SELECT
    i,
    avg(cast(v as interval)) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
FROM VALUES (1,'1 seconds'), (1,'2 seconds'), (2,NULL), (2,NULL) t(i,v);

-- Interval year-month arithmetic

create temporary view interval_arithmetic as
  select CAST(dateval AS date), CAST(tsval AS timestamp) from values
    ('2012-01-01', '2012-01-01')
    as interval_arithmetic(dateval, tsval);

select
  dateval,
  dateval - interval '2-2' year to month,
  dateval - interval '-2-2' year to month,
  dateval + interval '2-2' year to month,
  dateval + interval '-2-2' year to month,
  - interval '2-2' year to month + dateval,
  interval '2-2' year to month + dateval
from interval_arithmetic;

select
  tsval,
  tsval - interval '2-2' year to month,
  tsval - interval '-2-2' year to month,
  tsval + interval '2-2' year to month,
  tsval + interval '-2-2' year to month,
  - interval '2-2' year to month + tsval,
  interval '2-2' year to month + tsval
from interval_arithmetic;

select
  interval '2-2' year to month + interval '3-3' year to month,
  interval '2-2' year to month - interval '3-3' year to month
from interval_arithmetic;

-- Interval day-time arithmetic

select
  dateval,
  dateval - interval '99 11:22:33.123456789' day to second,
  dateval - interval '-99 11:22:33.123456789' day to second,
  dateval + interval '99 11:22:33.123456789' day to second,
  dateval + interval '-99 11:22:33.123456789' day to second,
  -interval '99 11:22:33.123456789' day to second + dateval,
  interval '99 11:22:33.123456789' day to second + dateval
from interval_arithmetic;

select
  tsval,
  tsval - interval '99 11:22:33.123456789' day to second,
  tsval - interval '-99 11:22:33.123456789' day to second,
  tsval + interval '99 11:22:33.123456789' day to second,
  tsval + interval '-99 11:22:33.123456789' day to second,
  -interval '99 11:22:33.123456789' day to second + tsval,
  interval '99 11:22:33.123456789' day to second + tsval
from interval_arithmetic;

select
  interval '99 11:22:33.123456789' day to second + interval '10 9:8:7.123456789' day to second,
  interval '99 11:22:33.123456789' day to second - interval '10 9:8:7.123456789' day to second
from interval_arithmetic;

-- control characters as white spaces
select interval '\t interval 1 day';
select interval 'interval \t 1\tday';
select interval 'interval\t1\tday';
select interval '1\t' day;
select interval '1 ' day;

-- interval overflow if (ansi) exception else NULL
select -(a) from values (interval '-2147483648 months', interval '2147483647 months') t(a, b);
select a - b from values (interval '-2147483648 months', interval '2147483647 months') t(a, b);
select b + interval '1 month' from values (interval '-2147483648 months', interval '2147483647 months') t(a, b);
select a * 1.1 from values (interval '-2147483648 months', interval '2147483647 months') t(a, b);
select a / 0.5 from values (interval '-2147483648 months', interval '2147483647 months') t(a, b);
