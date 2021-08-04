-- timestamp_ltz functions

select timestamp_ltz'2019-10-06 10:11:12.345678';
select timestamp_ltz'-1969-12-31 16:00:00';
select timestamp_ltz'0015-03-18 16:00:00';
select timestamp_ltz'-000001';
select timestamp_ltz'99999-03-18T12:03:17';

select to_timestamp_ltz(null), to_timestamp_ltz('2016-12-31 00:12:00'), to_timestamp_ltz('2016-12-31', 'yyyy-MM-dd');
select to_timestamp_ltz(to_date(null)), to_timestamp_ltz(to_date('2016-12-31')), to_timestamp_ltz(to_date('2016-12-31', 'yyyy-MM-dd'));
select to_timestamp_ltz(to_timestamp(null)), to_timestamp_ltz(to_timestamp('2016-12-31 00:12:00')), to_timestamp_ltz(to_timestamp('2016-12-31', 'yyyy-MM-dd'));

-- TimestampLTZ numeric fields constructor
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 45.678);
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 45.678, 'CET');
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 60.007);
