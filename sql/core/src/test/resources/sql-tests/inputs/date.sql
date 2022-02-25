-- date literals, functions and operations

create temporary view date_view as select '2011-11-11' date_str, '1' int_str;

select date '2019-01-01\t';
select date '2020-01-01中文';

select make_date(2019, 1, 1), make_date(12, 12, 12);
-- invalid month
select make_date(2000, 13, 1);
-- invalid day
select make_date(2000, 1, 33);

-- invalid: year field must have at least 4 digits
select date'015';
-- invalid: month field can have at most 2 digits
select date'2021-4294967297-11';

select current_date = current_date;
-- under ANSI mode, `current_date` can't be a function name.
select current_date() = current_date();

-- conversions between date and unix_date (number of days from epoch)
select DATE_FROM_UNIX_DATE(0), DATE_FROM_UNIX_DATE(1000), DATE_FROM_UNIX_DATE(null);
select UNIX_DATE(DATE('1970-01-01')), UNIX_DATE(DATE('2020-12-04')), UNIX_DATE(null);

select to_date(null), to_date('2016-12-31'), to_date('2016-12-31', 'yyyy-MM-dd');

-- missing fields in `to_date`
select to_date("16", "dd");
-- invalid: there is no 29 in February, 1970
select to_date("02-29", "MM-dd");

-- `dayofweek` accepts both date and timestamp ltz/ntz inputs.
select dayofweek('2007-02-03'), dayofweek('2009-07-30'), dayofweek('2017-05-27'), dayofweek(null),
  dayofweek('1582-10-15 13:10:15'), dayofweek(timestamp_ltz'1582-10-15 13:10:15'), dayofweek(timestamp_ntz'1582-10-15 13:10:15');

-- `weekday` accepts both date and timestamp ltz/ntz inputs.
select weekday('2007-02-03'), weekday('2009-07-30'), weekday('2017-05-27'), weekday(null),
  weekday('1582-10-15 13:10:15'), weekday(timestamp_ltz'1582-10-15 13:10:15'), weekday(timestamp_ntz'1582-10-15 13:10:15');

-- `year` accepts both date and timestamp ltz/ntz inputs.
select year('1500-01-01'), year('1582-10-15 13:10:15'), year(timestamp_ltz'1582-10-15 13:10:15'), year(timestamp_ntz'1582-10-15 13:10:15');

-- `month` accepts both date and timestamp ltz/ntz inputs.
select month('1500-01-01'), month('1582-10-15 13:10:15'), month(timestamp_ltz'1582-10-15 13:10:15'), month(timestamp_ntz'1582-10-15 13:10:15');

-- `dayOfYear` accepts both date and timestamp ltz/ntz inputs.
select dayOfYear('1500-01-01'), dayOfYear('1582-10-15 13:10:15'), dayOfYear(timestamp_ltz'1582-10-15 13:10:15'), dayOfYear(timestamp_ntz'1582-10-15 13:10:15');

-- next_day
select next_day("2015-07-23", "Mon");
select next_day("2015-07-23", "xx");
select next_day("2015-07-23 12:12:12", "Mon");
-- next_date does not accept timestamp lzt/ntz input
select next_day(timestamp_ltz"2015-07-23 12:12:12", "Mon");
select next_day(timestamp_ntz"2015-07-23 12:12:12", "Mon");
select next_day("xx", "Mon");
select next_day(null, "Mon");
select next_day(null, "xx");

-- date add
select date_add(date'2011-11-11', 1);
select date_add('2011-11-11', 1);
select date_add('2011-11-11', 1Y);
select date_add('2011-11-11', 1S);
-- invalid cases: the second parameter can only be byte/short/int
select date_add('2011-11-11', 1L);
select date_add('2011-11-11', 1.0);
select date_add('2011-11-11', 1E1);
-- the second parameter can be a string literal if it can be parsed to int
select date_add('2011-11-11', '1');
select date_add('2011-11-11', '1.2');
-- null input leads to null result.
select date_add(null, 1);
select date_add(date'2011-11-11', null);
-- `date_add` accepts both date and timestamp ltz/ntz inputs (non-ANSI mode).
select date_add(timestamp_ltz'2011-11-11 12:12:12', 1);
select date_add(timestamp_ntz'2011-11-11 12:12:12', 1);

-- date sub
select date_sub(date'2011-11-11', 1);
select date_sub('2011-11-11', 1);
select date_sub('2011-11-11', 1Y);
select date_sub('2011-11-11', 1S);
-- invalid cases: the second parameter can only be byte/short/int
select date_sub('2011-11-11', 1L);
select date_sub('2011-11-11', 1.0);
select date_sub('2011-11-11', 1E1);
-- the second parameter can be a string literal if it can be parsed to int
select date_sub(date'2011-11-11', '1');
select date_sub(date'2011-11-11', '1.2');
-- null input leads to null result.
select date_sub(null, 1);
select date_sub(date'2011-11-11', null);
-- `date_sub` accepts both date and timestamp ltz/ntz inputs (non-ANSI mode).
select date_sub(timestamp_ltz'2011-11-11 12:12:12', 1);
select date_sub(timestamp_ntz'2011-11-11 12:12:12', 1);

-- date add/sub with non-literal string column
select date_add('2011-11-11', int_str) from date_view;
select date_sub('2011-11-11', int_str) from date_view;
select date_add(date_str, 1) from date_view;
select date_sub(date_str, 1) from date_view;

-- date +/- number
select date '2011-11-11' + 1E1;
select date '2001-09-28' + 7Y;
select 7S + date '2001-09-28';
select date '2001-10-01' - 7;

-- date - date
select date '2001-10-01' - date '2001-09-28';
-- if one side is string/null literal, promote it to date type.
select date '2001-10-01' - '2001-09-28';
select '2001-10-01' - date '2001-09-28';
select date '2001-09-28' - null;
select null - date '2019-10-06';
-- invalid: non-literal string column
select date_str - date '2001-09-28' from date_view;
select date '2001-09-28' - date_str from date_view;

-- invalid: date + string literal
select date'2011-11-11' + '1';
select '1' + date'2011-11-11';

-- null result: date + null
select date'2011-11-11' + null;
select null + date'2011-11-11';

-- date +/- interval and interval + date
select date '2012-01-01' - interval '2-2' year to month,
       date '2011-11-11' - interval '2' day,
       date '2012-01-01' + interval '-2-2' year to month,
       date '2011-11-11' + interval '-2' month,
       - interval '2-2' year to month + date '2012-01-01',
       interval '-2' day + date '2011-11-11';

-- Unsupported narrow text style
select to_date('26/October/2015', 'dd/MMMMM/yyyy');
select from_json('{"d":"26/October/2015"}', 'd Date', map('dateFormat', 'dd/MMMMM/yyyy'));
select from_csv('26/October/2015', 'd Date', map('dateFormat', 'dd/MMMMM/yyyy'));

-- Add a number of units to a timestamp or a date
select dateadd(MICROSECOND, 1001, timestamp'2022-02-25 01:02:03.123');
select date_add(MILLISECOND, -1, timestamp'2022-02-25 01:02:03.456');
select dateadd(SECOND, 58, timestamp'2022-02-25 01:02:03');
select date_add(MINUTE, -100, date'2022-02-25');
select dateadd(HOUR, -1, timestamp'2022-02-25 01:02:03');
select date_add(DAY, 367, date'2022-02-25');
select dateadd(WEEK, -4, timestamp'2022-02-25 01:02:03');
select date_add(MONTH, -1, timestamp'2022-02-25 01:02:03');
select dateadd(QUARTER, 5, date'2022-02-25');
select date_add(YEAR, 1, date'2022-02-25');
