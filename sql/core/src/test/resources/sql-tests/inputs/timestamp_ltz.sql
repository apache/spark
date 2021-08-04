-- timestamp_ltz functions
--CONFIG_DIM1 spark.sql.timestampType=TIMESTAMP_LTZ
--CONFIG_DIM1 spark.sql.timestampType=TIMESTAMP_NTZ

select timestamp_ltz'2019-10-06 10:11:12.345678';
select timestamp_ltz'0015-03-18 16:00:00';

select to_timestamp_ltz(null), to_timestamp_ltz('2016-12-31 00:12:00'), to_timestamp_ltz('2016-12-31', 'yyyy-MM-dd');
select to_timestamp_ltz(to_date(null)), to_timestamp_ltz(to_date('2016-12-31')), to_timestamp_ltz(to_date('2016-12-31', 'yyyy-MM-dd'));
select to_timestamp_ltz(to_timestamp_ltz(null)), to_timestamp_ltz(to_timestamp_ltz('2016-12-31 00:12:00')), to_timestamp_ltz(to_timestamp_ltz('2016-12-31', 'yyyy-MM-dd'));
select to_timestamp_ltz(to_timestamp_ntz(null)), to_timestamp_ltz(to_timestamp_ntz('2016-12-31 00:12:00')), to_timestamp_ltz(to_timestamp_ntz('2016-12-31', 'yyyy-MM-dd'));

-- TimestampLTZ numeric fields constructor
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 45.678);
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 45.678, 'CET');
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 60.007);
