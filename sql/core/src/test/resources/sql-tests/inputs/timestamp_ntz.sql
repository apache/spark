-- timestamp_ntz functions

select timestamp_ntz'2019-10-06 10:11:12.345678';
select timestamp_ntz'-1969-12-31 16:00:00';
select timestamp_ntz'0015-03-18 16:00:00';
select timestamp_ntz'-000001';
select timestamp_ntz'99999-03-18T12:03:17';

select to_timestamp_ntz(null), to_timestamp_ntz('2016-12-31 00:12:00'), to_timestamp_ntz('2016-12-31', 'yyyy-MM-dd');
select to_timestamp_ntz(to_date(null)), to_timestamp_ntz(to_date('2016-12-31')), to_timestamp_ntz(to_date('2016-12-31', 'yyyy-MM-dd'));
select to_timestamp_ntz(to_timestamp(null)), to_timestamp_ntz(to_timestamp('2016-12-31 00:12:00')), to_timestamp_ntz(to_timestamp('2016-12-31', 'yyyy-MM-dd'));

-- subtract timestamps without time zone
select date'2020-01-01' - to_timestamp_ntz('2019-10-06 10:11:12.345678');
select to_timestamp_ntz('2019-10-06 10:11:12.345678') - date'2020-01-01';
select to_timestamp_ntz('2019-10-06 10:11:12.345678') - null;
select null - to_timestamp_ntz('2019-10-06 10:11:12.345678');
select to_timestamp_ntz('2019-10-07 10:11:12.345678') - to_timestamp_ntz('2019-10-06 10:11:12.345677');
select to_timestamp_ntz('2019-10-06 10:11:12.345677') - to_timestamp_ntz('2019-10-07 10:11:12.345678');
select to_timestamp_ntz('2019-10-07 10:11:12.345678') - to_timestamp('2019-10-06 10:11:12.345678');
select to_timestamp('2019-10-06 10:11:12.345678') - to_timestamp_ntz('2019-10-07 10:11:12.345678');

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
select to_timestamp_ntz("16", "dd");
select to_timestamp_ntz("02-29", "MM-dd");
select to_timestamp_ntz("2019 40", "yyyy mm");
select to_timestamp_ntz("2019 10:10:10", "yyyy hh:mm:ss");

-- Unsupported narrow text style
select to_timestamp_ntz('2019-10-06 A', 'yyyy-MM-dd GGGGG');
select to_timestamp_ntz('22 05 2020 Friday', 'dd MM yyyy EEEEEE');
select to_timestamp_ntz('22 05 2020 Friday', 'dd MM yyyy EEEEE');

-- Datetime types parse error
select to_timestamp_ntz("2020-01-27T20:06:11.847", "yyyy-MM-dd HH:mm:ss.SSS");
select to_timestamp_ntz("Unparseable", "yyyy-MM-dd HH:mm:ss.SSS");

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

-- TimestampNTZ numeric fields constructor
SELECT make_timestamp_ntz(2021, 07, 11, 6, 30, 45.678);
-- make_timestamp_ntz should not accept time zone input
SELECT make_timestamp_ntz(2021, 07, 11, 6, 30, 45.678, 'CET');
SELECT make_timestamp_ntz(2021, 07, 11, 6, 30, 60.007);
