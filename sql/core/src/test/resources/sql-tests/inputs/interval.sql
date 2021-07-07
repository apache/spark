-- test for intervals

-- multiply and divide an interval by a number
select 3 * (timestamp'2019-10-15 10:11:12.001002' - date'2019-10-15');
select interval 4 month 2 weeks 3 microseconds * 1.5;
select (timestamp'2019-10-15' - timestamp'2019-10-14') / 1.5;
select interval 2147483647 month * 2;
select interval 2147483647 month / 0.5;
select interval 2147483647 day * 2;
select interval 2147483647 day / 0.5;

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
select interval -'1-1' year to month;
select interval -'-1-1' year to month;
select interval +'-1-1' year to month;
select interval - '1 2:3:4.001' day to second;
select interval +'1 2:3:4.001' day to second;
select interval -'-1 2:3:4.001' day to second;

-- make intervals
select make_interval(1);
select make_interval(1, 2);
select make_interval(1, 2, 3);
select make_interval(1, 2, 3, 4);
select make_interval(1, 2, 3, 4, 5);
select make_interval(1, 2, 3, 4, 5, 6);
select make_interval(1, 2, 3, 4, 5, 6, 7.008009);
select make_interval(1, 2, 3, 4, 0, 0, 123456789012.123456);
select make_interval(0, 0, 0, 0, 0, 0, 1234567890123456789);

-- make_dt_interval
select make_dt_interval(1);
select make_dt_interval(1, 2);
select make_dt_interval(1, 2, 3);
select make_dt_interval(1, 2, 3, 4.005006);
select make_dt_interval(1, 0, 0, 123456789012.123456);
select make_dt_interval(2147483647);

-- make_ym_interval
select make_ym_interval(1);
select make_ym_interval(1, 2);
select make_ym_interval(0, 1);
select make_ym_interval(178956970, 7);
select make_ym_interval(178956970, 8);
select make_ym_interval(-178956970, -8);
select make_ym_interval(-178956970, -9);

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
select interval '0-0' year to month;
select interval '0 0:0:0' day to second;
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
select interval 30 days days;

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
SELECT interval '1 year 3 months 2 weeks 2 days 1 hour 3 minutes 2 seconds 100 millisecond 200 microseconds';

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
select interval (-30) days;
select interval (a + 1) days;
select interval 30 days days days;
SELECT INTERVAL '178956970-7' YEAR TO MONTH;
SELECT INTERVAL '178956970-8' YEAR TO MONTH;
SELECT INTERVAL '-178956970-8' YEAR TO MONTH;
SELECT INTERVAL -'178956970-8' YEAR TO MONTH;

-- Interval year-month arithmetic

create temporary view interval_arithmetic as
  select CAST(dateval AS date), CAST(tsval AS timestamp), dateval as strval from values
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

-- datetimes(in string representation) + intervals
select
  strval,
  strval - interval '99 11:22:33.123456789' day to second,
  strval - interval '-99 11:22:33.123456789' day to second,
  strval + interval '99 11:22:33.123456789' day to second,
  strval + interval '-99 11:22:33.123456789' day to second,
  -interval '99 11:22:33.123456789' day to second + strval,
  interval '99 11:22:33.123456789' day to second + strval
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
select interval '2-2\t' year to month;
select interval '-\t2-2\t' year to month;
select interval '\n0 12:34:46.789\t' day to second;
select interval '\n-\t10\t 12:34:46.789\t' day to second;
select interval '中文 interval 1 day';
select interval 'interval中文 1 day';
select interval 'interval 1中文day';


-- interval overflow if (ansi) exception else NULL
select -(a) from values (interval '-2147483648 months', interval '2147483647 months') t(a, b);
select a - b from values (interval '-2147483648 months', interval '2147483647 months') t(a, b);
select b + interval '1 month' from values (interval '-2147483648 months', interval '2147483647 months') t(a, b);
select a * 1.1 from values (interval '-2147483648 months', interval '2147483647 months') t(a, b);
select a / 0.5 from values (interval '-2147483648 months', interval '2147483647 months') t(a, b);

-- interval support for csv and json functions
SELECT
  from_csv('1, 1 day', 'a INT, b interval'),
  to_csv(from_csv('1, 1 day', 'a INT, b interval')),
  to_csv(named_struct('a', interval 32 month, 'b', interval 70 minute)),
  from_csv(to_csv(named_struct('a', interval 32 month, 'b', interval 70 minute)), 'a interval, b interval');
SELECT
  from_json('{"a":"1 days"}', 'a interval'),
  to_json(from_json('{"a":"1 days"}', 'a interval')),
  to_json(map('a', interval 25 month 100 day 130 minute)),
  from_json(to_json(map('a', interval 25 month 100 day 130 minute)), 'a interval');

select interval '+';
select interval '+.';
select interval '1';
select interval '1.2';
select interval '- 2';
select interval '1 day -';
select interval '1 day 1';

select interval '1 day 2' day;
select interval 'interval 1' day;
select interval '-\t 1' day;

SELECT (INTERVAL '-178956970-8' YEAR TO MONTH) / 2;
SELECT (INTERVAL '-178956970-8' YEAR TO MONTH) / 5;
SELECT (INTERVAL '-178956970-8' YEAR TO MONTH) / -1;
SELECT (INTERVAL '-178956970-8' YEAR TO MONTH) / -1L;
SELECT (INTERVAL '-178956970-8' YEAR TO MONTH) / -1.0;
SELECT (INTERVAL '-178956970-8' YEAR TO MONTH) / -1.0D;

SELECT (INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND) / 2;
SELECT (INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND) / 5;
SELECT (INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND) / -1;
SELECT (INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND) / -1L;
SELECT (INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND) / -1.0;
SELECT (INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND) / -1.0D;

SELECT INTERVAL '106751991 04' DAY TO HOUR;
SELECT INTERVAL '106751991 04:00' DAY TO MINUTE;
SELECT INTERVAL '106751991 04:00:54.775807' DAY TO SECOND;
SELECT INTERVAL '2562047788:00' HOUR TO MINUTE;
SELECT INTERVAL '2562047788:00:54.775807' HOUR TO SECOND;
SELECT INTERVAL '153722867280:54.775807' MINUTE TO SECOND;
SELECT INTERVAL '-106751991 04' DAY TO HOUR;
SELECT INTERVAL '-106751991 04:00' DAY TO MINUTE;
SELECT INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND;
SELECT INTERVAL '-2562047788:00' HOUR TO MINUTE;
SELECT INTERVAL '-2562047788:00:54.775808' HOUR TO SECOND;
SELECT INTERVAL '-153722867280:54.775808' MINUTE TO SECOND;

SELECT INTERVAL '106751992 04' DAY TO HOUR;
SELECT INTERVAL '-106751992 04' DAY TO HOUR;
SELECT INTERVAL '2562047789:00' HOUR TO MINUTE;
SELECT INTERVAL '-2562047789:00' HOUR TO MINUTE;
SELECT INTERVAL '153722867281:54.775808' MINUTE TO SECOND;
SELECT INTERVAL '-153722867281:54.775808' MINUTE TO SECOND;

