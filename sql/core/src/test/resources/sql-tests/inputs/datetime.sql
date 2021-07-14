-- date time functions

-- [SPARK-31710] TIMESTAMP_SECONDS, TIMESTAMP_MILLISECONDS and TIMESTAMP_MICROSECONDS to timestamp transfer
select TIMESTAMP_SECONDS(1230219000),TIMESTAMP_SECONDS(-1230219000),TIMESTAMP_SECONDS(null);
select TIMESTAMP_SECONDS(1.23), TIMESTAMP_SECONDS(1.23d), TIMESTAMP_SECONDS(FLOAT(1.23));
select TIMESTAMP_MILLIS(1230219000123),TIMESTAMP_MILLIS(-1230219000123),TIMESTAMP_MILLIS(null);
select TIMESTAMP_MICROS(1230219000123123),TIMESTAMP_MICROS(-1230219000123123),TIMESTAMP_MICROS(null);
-- overflow exception
select TIMESTAMP_SECONDS(1230219000123123);
select TIMESTAMP_SECONDS(-1230219000123123);
select TIMESTAMP_MILLIS(92233720368547758);
select TIMESTAMP_MILLIS(-92233720368547758);
-- truncate exception
select TIMESTAMP_SECONDS(0.1234567);
-- truncation is OK for float/double
select TIMESTAMP_SECONDS(0.1234567d), TIMESTAMP_SECONDS(FLOAT(0.1234567));
-- UNIX_SECONDS, UNIX_MILLISECONDS and UNIX_MICROSECONDS
select UNIX_SECONDS(TIMESTAMP('2020-12-01 14:30:08Z')), UNIX_SECONDS(TIMESTAMP('2020-12-01 14:30:08.999999Z')), UNIX_SECONDS(null);
select UNIX_MILLIS(TIMESTAMP('2020-12-01 14:30:08Z')), UNIX_MILLIS(TIMESTAMP('2020-12-01 14:30:08.999999Z')), UNIX_MILLIS(null);
select UNIX_MICROS(TIMESTAMP('2020-12-01 14:30:08Z')), UNIX_MICROS(TIMESTAMP('2020-12-01 14:30:08.999999Z')), UNIX_MICROS(null);
-- DATE_FROM_UNIX_DATE
select DATE_FROM_UNIX_DATE(0), DATE_FROM_UNIX_DATE(1000), DATE_FROM_UNIX_DATE(null);
-- UNIX_DATE
select UNIX_DATE(DATE('1970-01-01')), UNIX_DATE(DATE('2020-12-04')), UNIX_DATE(null);
-- [SPARK-16836] current_date and current_timestamp literals
select current_date = current_date(), current_timestamp = current_timestamp();
select localtimestamp() = localtimestamp();

select to_date(null), to_date('2016-12-31'), to_date('2016-12-31', 'yyyy-MM-dd');

select to_timestamp(null), to_timestamp('2016-12-31 00:12:00'), to_timestamp('2016-12-31', 'yyyy-MM-dd');

select to_timestamp_ntz(null), to_timestamp_ntz('2016-12-31 00:12:00'), to_timestamp_ntz('2016-12-31', 'yyyy-MM-dd');
select to_timestamp_ntz(to_date(null)), to_timestamp_ntz(to_date('2016-12-31')), to_timestamp_ntz(to_date('2016-12-31', 'yyyy-MM-dd'));
select to_timestamp_ntz(to_timestamp(null)), to_timestamp_ntz(to_timestamp('2016-12-31 00:12:00')), to_timestamp_ntz(to_timestamp('2016-12-31', 'yyyy-MM-dd'));

select to_timestamp_ltz(null), to_timestamp_ltz('2016-12-31 00:12:00'), to_timestamp_ltz('2016-12-31', 'yyyy-MM-dd');
select to_timestamp_ltz(to_date(null)), to_timestamp_ltz(to_date('2016-12-31')), to_timestamp_ltz(to_date('2016-12-31', 'yyyy-MM-dd'));
select to_timestamp_ltz(to_timestamp(null)), to_timestamp_ltz(to_timestamp('2016-12-31 00:12:00')), to_timestamp_ltz(to_timestamp('2016-12-31', 'yyyy-MM-dd'));

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


select date '2019-01-01\t';
select timestamp '2019-01-01\t';
select date '2020-01-01中文';
select timestamp '2019-01-01中文';

-- time add/sub
select timestamp'2011-11-11 11:11:11' + interval '2' day;
select timestamp'2011-11-11 11:11:11' - interval '2' day;
select date'2011-11-11 11:11:11' + interval '2' second;
select date'2011-11-11 11:11:11' - interval '2' second;
select '2011-11-11' - interval '2' day;
select '2011-11-11 11:11:11' - interval '2' second;
select '1' - interval '2' second;
select 1 - interval '2' second;

-- subtract timestamps
select date'2020-01-01' - timestamp'2019-10-06 10:11:12.345678';
select timestamp'2019-10-06 10:11:12.345678' - date'2020-01-01';
select timestamp'2019-10-06 10:11:12.345678' - null;
select null - timestamp'2019-10-06 10:11:12.345678';

-- subtract timestamps without time zone
select date'2020-01-01' - to_timestamp_ntz('2019-10-06 10:11:12.345678');
select to_timestamp_ntz('2019-10-06 10:11:12.345678') - date'2020-01-01';
select to_timestamp_ntz('2019-10-06 10:11:12.345678') - null;
select null - to_timestamp_ntz('2019-10-06 10:11:12.345678');
select to_timestamp_ntz('2019-10-07 10:11:12.345678') - to_timestamp_ntz('2019-10-06 10:11:12.345677');
select to_timestamp_ntz('2019-10-06 10:11:12.345677') - to_timestamp_ntz('2019-10-07 10:11:12.345678');
select to_timestamp_ntz('2019-10-07 10:11:12.345678') - to_timestamp('2019-10-06 10:11:12.345678');
select to_timestamp('2019-10-06 10:11:12.345678') - to_timestamp_ntz('2019-10-07 10:11:12.345678');

-- date add/sub
select date_add('2011-11-11', 1Y);
select date_add('2011-11-11', 1S);
select date_add('2011-11-11', 1);
select date_add('2011-11-11', 1L);
select date_add('2011-11-11', 1.0);
select date_add('2011-11-11', 1E1);
select date_add('2011-11-11', '1');
select date_add('2011-11-11', '1.2');
select date_add(date'2011-11-11', 1);
select date_add(timestamp'2011-11-11', 1);
select date_sub(date'2011-11-11', 1);
select date_sub(date'2011-11-11', '1');
select date_sub(date'2011-11-11', '1.2');
select date_sub(timestamp'2011-11-11', 1);
select date_sub(null, 1);
select date_sub(date'2011-11-11', null);
select date'2011-11-11' + 1E1;
select date'2011-11-11' + '1';
select null + date '2001-09-28';
select date '2001-09-28' + 7Y;
select 7S + date '2001-09-28';
select date '2001-10-01' - 7;
select date '2001-10-01' - '7';
select date '2001-09-28' + null;
select date '2001-09-28' - null;

-- date add/sub with non-literal string column
create temp view v as select '1' str;
select date_add('2011-11-11', str) from v;
select date_sub('2011-11-11', str) from v;

-- subtract dates
select null - date '2019-10-06';
select date '2001-10-01' - date '2001-09-28';

-- variable-length second fraction tests
select to_timestamp('2019-10-06 10:11:12.', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.0', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.1', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.12', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.123UTC', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.1234', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.12345CST', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.123456PST', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
-- second fraction exceeded max variable length
select to_timestamp('2019-10-06 10:11:12.1234567PST', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
-- special cases
select to_timestamp('123456 2019-10-06 10:11:12.123456PST', 'SSSSSS yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('223456 2019-10-06 10:11:12.123456PST', 'SSSSSS yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.1234', 'yyyy-MM-dd HH:mm:ss.[SSSSSS]');
select to_timestamp('2019-10-06 10:11:12.123', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]');
select to_timestamp('2019-10-06 10:11:12', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]');
select to_timestamp('2019-10-06 10:11:12.12', 'yyyy-MM-dd HH:mm[:ss.SSSSSS]');
select to_timestamp('2019-10-06 10:11', 'yyyy-MM-dd HH:mm[:ss.SSSSSS]');
select to_timestamp("2019-10-06S10:11:12.12345", "yyyy-MM-dd'S'HH:mm:ss.SSSSSS");
select to_timestamp("12.12342019-10-06S10:11", "ss.SSSSyyyy-MM-dd'S'HH:mm");
select to_timestamp("12.1232019-10-06S10:11", "ss.SSSSyyyy-MM-dd'S'HH:mm");
select to_timestamp("12.1232019-10-06S10:11", "ss.SSSSyy-MM-dd'S'HH:mm");
select to_timestamp("12.1234019-10-06S10:11", "ss.SSSSy-MM-dd'S'HH:mm");

select to_timestamp("2019-10-06S", "yyyy-MM-dd'S'");
select to_timestamp("S2019-10-06", "'S'yyyy-MM-dd");

select to_timestamp("2019-10-06T10:11:12'12", "yyyy-MM-dd'T'HH:mm:ss''SSSS"); -- middle
select to_timestamp("2019-10-06T10:11:12'", "yyyy-MM-dd'T'HH:mm:ss''"); -- tail
select to_timestamp("'2019-10-06T10:11:12", "''yyyy-MM-dd'T'HH:mm:ss"); -- head
select to_timestamp("P2019-10-06T10:11:12", "'P'yyyy-MM-dd'T'HH:mm:ss"); -- head but as single quote

-- variable-length second fraction tests
select to_timestamp_ntz('2019-10-06 10:11:12.', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp_ntz('2019-10-06 10:11:12.0', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp_ntz('2019-10-06 10:11:12.1', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp_ntz('2019-10-06 10:11:12.12', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp_ntz('2019-10-06 10:11:12.123UTC', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp_ntz('2019-10-06 10:11:12.1234', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp_ntz('2019-10-06 10:11:12.12345CST', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp_ntz('2019-10-06 10:11:12.123456PST', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
-- second fraction exceeded max variable length
select to_timestamp_ntz('2019-10-06 10:11:12.1234567PST', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
-- special cases
select to_timestamp_ntz('123456 2019-10-06 10:11:12.123456PST', 'SSSSSS yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp_ntz('223456 2019-10-06 10:11:12.123456PST', 'SSSSSS yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp_ntz('2019-10-06 10:11:12.1234', 'yyyy-MM-dd HH:mm:ss.[SSSSSS]');
select to_timestamp_ntz('2019-10-06 10:11:12.123', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]');
select to_timestamp_ntz('2019-10-06 10:11:12', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]');
select to_timestamp_ntz('2019-10-06 10:11:12.12', 'yyyy-MM-dd HH:mm[:ss.SSSSSS]');
select to_timestamp_ntz('2019-10-06 10:11', 'yyyy-MM-dd HH:mm[:ss.SSSSSS]');
select to_timestamp_ntz("2019-10-06S10:11:12.12345", "yyyy-MM-dd'S'HH:mm:ss.SSSSSS");
select to_timestamp_ntz("12.12342019-10-06S10:11", "ss.SSSSyyyy-MM-dd'S'HH:mm");
select to_timestamp_ntz("12.1232019-10-06S10:11", "ss.SSSSyyyy-MM-dd'S'HH:mm");
select to_timestamp_ntz("12.1232019-10-06S10:11", "ss.SSSSyy-MM-dd'S'HH:mm");
select to_timestamp_ntz("12.1234019-10-06S10:11", "ss.SSSSy-MM-dd'S'HH:mm");

select to_timestamp_ntz("2019-10-06S", "yyyy-MM-dd'S'");
select to_timestamp_ntz("S2019-10-06", "'S'yyyy-MM-dd");

select to_timestamp_ntz("2019-10-06T10:11:12'12", "yyyy-MM-dd'T'HH:mm:ss''SSSS"); -- middle
select to_timestamp_ntz("2019-10-06T10:11:12'", "yyyy-MM-dd'T'HH:mm:ss''"); -- tail
select to_timestamp_ntz("'2019-10-06T10:11:12", "''yyyy-MM-dd'T'HH:mm:ss"); -- head
select to_timestamp_ntz("P2019-10-06T10:11:12", "'P'yyyy-MM-dd'T'HH:mm:ss"); -- head but as single quote

-- missing fields
select to_timestamp("16", "dd");
select to_timestamp("02-29", "MM-dd");
select to_timestamp_ntz("16", "dd");
select to_timestamp_ntz("02-29", "MM-dd");
select to_date("16", "dd");
select to_date("02-29", "MM-dd");
select to_timestamp("2019 40", "yyyy mm");
select to_timestamp("2019 10:10:10", "yyyy hh:mm:ss");
select to_timestamp_ntz("2019 40", "yyyy mm");
select to_timestamp_ntz("2019 10:10:10", "yyyy hh:mm:ss");

-- Unsupported narrow text style
select to_timestamp('2019-10-06 A', 'yyyy-MM-dd GGGGG');
select to_timestamp('22 05 2020 Friday', 'dd MM yyyy EEEEEE');
select to_timestamp('22 05 2020 Friday', 'dd MM yyyy EEEEE');
select to_timestamp_ntz('2019-10-06 A', 'yyyy-MM-dd GGGGG');
select to_timestamp_ntz('22 05 2020 Friday', 'dd MM yyyy EEEEEE');
select to_timestamp_ntz('22 05 2020 Friday', 'dd MM yyyy EEEEE');
select unix_timestamp('22 05 2020 Friday', 'dd MM yyyy EEEEE');
select from_json('{"t":"26/October/2015"}', 't Timestamp', map('timestampFormat', 'dd/MMMMM/yyyy'));
select from_json('{"d":"26/October/2015"}', 'd Date', map('dateFormat', 'dd/MMMMM/yyyy'));
select from_csv('26/October/2015', 't Timestamp', map('timestampFormat', 'dd/MMMMM/yyyy'));
select from_csv('26/October/2015', 'd Date', map('dateFormat', 'dd/MMMMM/yyyy'));

-- Datetime types parse error
select to_date("2020-01-27T20:06:11.847", "yyyy-MM-dd HH:mm:ss.SSS");
select to_date("Unparseable", "yyyy-MM-dd HH:mm:ss.SSS");
select to_timestamp("2020-01-27T20:06:11.847", "yyyy-MM-dd HH:mm:ss.SSS");
select to_timestamp("Unparseable", "yyyy-MM-dd HH:mm:ss.SSS");
select to_timestamp_ntz("2020-01-27T20:06:11.847", "yyyy-MM-dd HH:mm:ss.SSS");
select to_timestamp_ntz("Unparseable", "yyyy-MM-dd HH:mm:ss.SSS");
select unix_timestamp("2020-01-27T20:06:11.847", "yyyy-MM-dd HH:mm:ss.SSS");
select unix_timestamp("Unparseable", "yyyy-MM-dd HH:mm:ss.SSS");
select to_unix_timestamp("2020-01-27T20:06:11.847", "yyyy-MM-dd HH:mm:ss.SSS");
select to_unix_timestamp("Unparseable", "yyyy-MM-dd HH:mm:ss.SSS");
select cast("Unparseable" as timestamp);
select cast("Unparseable" as date);

-- next_day
select next_day("2015-07-23", "Mon");
select next_day("2015-07-23", "xx");
select next_day("xx", "Mon");
select next_day(null, "Mon");
select next_day(null, "xx");

-- TimestampNTZ + Intervals
select to_timestamp_ntz('2021-06-25 10:11:12') + interval 2 day;
select to_timestamp_ntz('2021-06-25 10:11:12') + interval '0-0' year to month;
select to_timestamp_ntz('2021-06-25 10:11:12') + interval '1-2' year to month;
select to_timestamp_ntz('2021-06-25 10:11:12') + interval '0 0:0:0' day to second;
select to_timestamp_ntz('2021-06-25 10:11:12') + interval '0 0:0:0.1' day to second;
select to_timestamp_ntz('2021-06-25 10:11:12') + interval '10-9' year to month;
select to_timestamp_ntz('2021-06-25 10:11:12') + interval '20 15' day to hour;
select to_timestamp_ntz('2021-06-25 10:11:12') + interval '20 15:40' day to minute;
select to_timestamp_ntz('2021-06-25 10:11:12') + interval '20 15:40:32.99899999' day to second;

-- TimestampNTZ - Intervals
select to_timestamp_ntz('2021-06-25 10:11:12') - interval 2 day;
select to_timestamp_ntz('2021-06-25 10:11:12') - interval '0-0' year to month;
select to_timestamp_ntz('2021-06-25 10:11:12') - interval '1-2' year to month;
select to_timestamp_ntz('2021-06-25 10:11:12') - interval '0 0:0:0' day to second;
select to_timestamp_ntz('2021-06-25 10:11:12') - interval '0 0:0:0.1' day to second;
select to_timestamp_ntz('2021-06-25 10:11:12') - interval '10-9' year to month;
select to_timestamp_ntz('2021-06-25 10:11:12') - interval '20 15' day to hour;
select to_timestamp_ntz('2021-06-25 10:11:12') - interval '20 15:40' day to minute;
select to_timestamp_ntz('2021-06-25 10:11:12') - interval '20 15:40:32.99899999' day to second;

-- timestamp numeric fields constructor
SELECT make_timestamp(2021, 07, 11, 6, 30, 45.678);
SELECT make_timestamp(2021, 07, 11, 6, 30, 45.678, 'CET');
SELECT make_timestamp(2021, 07, 11, 6, 30, 60.007);

-- TimestampNTZ numeric fields constructor
SELECT make_timestamp_ntz(2021, 07, 11, 6, 30, 45.678);
-- make_timestamp_ntz should not accept time zone input
SELECT make_timestamp_ntz(2021, 07, 11, 6, 30, 45.678, 'CET');
SELECT make_timestamp_ntz(2021, 07, 11, 6, 30, 60.007);

-- TimestampLTZ numeric fields constructor
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 45.678);
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 45.678, 'CET');
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 60.007);

-- datetime with year outside [0000-9999]
select date'999999-03-18';
select date'-0001-1-28';
select date'0015';
select cast('015' as date);
select cast('2021-4294967297-11' as date);

select timestamp'-1969-12-31 16:00:00';
select timestamp'0015-03-18 16:00:00';
select timestamp'-000001';
select timestamp'99999-03-18T12:03:17';
select cast('4294967297' as timestamp);
select cast('2021-01-01T12:30:4294967297.123456' as timestamp);

