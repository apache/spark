-- timestamp_ntz literals and constructors
--CONFIG_DIM1 spark.sql.timestampType=TIMESTAMP_LTZ
--CONFIG_DIM1 spark.sql.timestampType=TIMESTAMP_NTZ

select timestamp_ntz'2016-12-31 00:12:00', timestamp_ntz'2016-12-31';

select to_timestamp_ntz(null), to_timestamp_ntz('2016-12-31 00:12:00'), to_timestamp_ntz('2016-12-31', 'yyyy-MM-dd');
-- `to_timestamp_ntz` can also take date input
select to_timestamp_ntz(to_date(null)), to_timestamp_ntz(to_date('2016-12-31'));
-- `to_timestamp_ntz` can also take timestamp_ltz input
select to_timestamp_ntz(to_timestamp_ltz(null)), to_timestamp_ntz(to_timestamp_ltz('2016-12-31 00:12:00'));

-- TimestampNTZ numeric fields constructor
SELECT make_timestamp_ntz(2021, 07, 11, 6, 30, 45.678);
-- make_timestamp_ntz should not accept time zone input
SELECT make_timestamp_ntz(2021, 07, 11, 6, 30, 45.678, 'CET');
SELECT make_timestamp_ntz(2021, 07, 11, 6, 30, 60.007);

SELECT convert_timezone('Europe/Moscow', 'America/Los_Angeles', timestamp_ntz'2022-01-01 00:00:00');
