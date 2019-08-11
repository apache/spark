-- Turns on ANSI mode
SET spark.sql.parser.ansi.enabled=true;

select
  '1' second,
  2  seconds,
  '1' minute,
  2  minutes,
  '1' hour,
  2  hours,
  '1' day,
  2  days,
  '1' month,
  2  months,
  '1' year,
  2  years;

select
  interval '10-11' year to month,
  interval '10' year,
  interval '11' month;

select
  '10-11' year to month,
  '10' year,
  '11' month;

select
  interval '10 9:8:7.987654321' day to second,
  interval '10' day,
  interval '11' hour,
  interval '12' minute,
  interval '13' second,
  interval '13.123456789' second;

select
  '10 9:8:7.987654321' day to second,
  '10' day,
  '11' hour,
  '12' minute,
  '13' second,
  '13.123456789' second;

select map(1, interval 1 day, 2, interval 3 week);

select map(1, 1 day, 2, 3 week);

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
  dateval,
  dateval - '2-2' year to month,
  dateval - '-2-2' year to month,
  dateval + '2-2' year to month,
  dateval + '-2-2' year to month,
  - '2-2' year to month + dateval,
  '2-2' year to month + dateval
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
  tsval,
  tsval - '2-2' year to month,
  tsval - '-2-2' year to month,
  tsval + '2-2' year to month,
  tsval + '-2-2' year to month,
  - '2-2' year to month + tsval,
  '2-2' year to month + tsval
from interval_arithmetic;

select
  interval '2-2' year to month + interval '3-3' year to month,
  interval '2-2' year to month - interval '3-3' year to month
from interval_arithmetic;

select
  '2-2' year to month + '3-3' year to month,
  '2-2' year to month - '3-3' year to month
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
  dateval,
  dateval - '99 11:22:33.123456789' day to second,
  dateval - '-99 11:22:33.123456789' day to second,
  dateval + '99 11:22:33.123456789' day to second,
  dateval + '-99 11:22:33.123456789' day to second,
  - '99 11:22:33.123456789' day to second + dateval,
  '99 11:22:33.123456789' day to second + dateval
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
  tsval,
  tsval - '99 11:22:33.123456789' day to second,
  tsval - '-99 11:22:33.123456789' day to second,
  tsval + '99 11:22:33.123456789' day to second,
  tsval + '-99 11:22:33.123456789' day to second,
  - '99 11:22:33.123456789' day to second + tsval,
  '99 11:22:33.123456789' day to second + tsval
from interval_arithmetic;

select
  interval '99 11:22:33.123456789' day to second + interval '10 9:8:7.123456789' day to second,
  interval '99 11:22:33.123456789' day to second - interval '10 9:8:7.123456789' day to second
from interval_arithmetic;

select
  '99 11:22:33.123456789' day to second + '10 9:8:7.123456789' day to second,
  '99 11:22:33.123456789' day to second - '10 9:8:7.123456789' day to second
from interval_arithmetic;

-- More tests for interval syntax alternatives

select 30 day;

select 30 day day;

select 30 day day day;

select date '2012-01-01' - 30 day;

select date '2012-01-01' - 30 day day;

select date '2012-01-01' - 30 day day day;

select date '2012-01-01' + '-30' day;

select date '2012-01-01' + interval '-30' day;

-- Unsupported syntax for intervals

select date '2012-01-01' + interval (-30) day;

select date '2012-01-01' + (-30) day;

create temporary view t as select * from values (1), (2) as t(a);

select date '2012-01-01' + interval (a + 1) day from t;

select date '2012-01-01' + (a + 1) day from t;

-- Turns off ANSI mode
SET spark.sql.parser.ansi.enabled=false;
