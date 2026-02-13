-- timestamp_ltz literals and constructors
--CONFIG_DIM1 spark.sql.timestampType=TIMESTAMP_LTZ
--CONFIG_DIM2 spark.sql.timestampType=TIMESTAMP_NTZ

select timestamp_ltz'2016-12-31 00:12:00', timestamp_ltz'2016-12-31';

select to_timestamp_ltz(null), to_timestamp_ltz('2016-12-31 00:12:00'), to_timestamp_ltz('2016-12-31', 'yyyy-MM-dd');
-- `to_timestamp_ltz` can also take date input
select to_timestamp_ltz(to_date(null)), to_timestamp_ltz(to_date('2016-12-31'));
-- `to_timestamp_ltz` can also take timestamp_ntz input
select to_timestamp_ltz(to_timestamp_ntz(null)), to_timestamp_ltz(to_timestamp_ntz('2016-12-31 00:12:00'));

-- TimestampLTZ numeric fields constructor
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 45.678);
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 45.678, 'CET');
SELECT make_timestamp_ltz(2021, 07, 11, 6, 30, 60.007);

-- TimestampLTZ date/time fields constructor
SELECT make_timestamp_ltz(make_date(2021, 07, 11), make_time(6, 30, 45.678));
-- Handling NULL input.
SELECT make_timestamp_ltz(NULL, TIME'00:00:00');
SELECT make_timestamp_ltz(DATE'1970-01-01', NULL);
-- Handling invalid input.
SELECT make_timestamp_ltz(timestamp_ntz'2018-11-17 13:33:33', TIME'0:0:0');
SELECT make_timestamp_ltz(DATE'2025-06-20', timestamp_ntz'2018-11-17 13:33:33');
-- Optional timezone is ignored.
SELECT make_timestamp_ltz(make_date(2021, 07, 11), make_time(6, 30, 45.678), 'PST');
SELECT make_timestamp_ltz(make_date(2021, 07, 11), make_time(6, 30, 45.678), 'CET');

SELECT convert_timezone('Europe/Brussels', timestamp_ltz'2022-03-23 00:00:00 America/Los_Angeles');

-- Try TimestampLTZ date/time fields constructor
SELECT try_make_timestamp_ltz(make_date(2021, 07, 11), make_time(6, 30, 45.678));
-- Handling NULL input.
SELECT try_make_timestamp_ltz(NULL, TIME'00:00:00');
SELECT try_make_timestamp_ltz(DATE'1970-01-01', NULL);
-- Handling invalid input.
SELECT try_make_timestamp_ltz(timestamp_ntz'2018-11-17 13:33:33', TIME'0:0:0');
SELECT try_make_timestamp_ltz(DATE'2025-06-20', timestamp_ntz'2018-11-17 13:33:33');
-- Optional timezone is ignored.
SELECT try_make_timestamp_ltz(make_date(2021, 07, 11), make_time(6, 30, 45.678), 'PST');
SELECT try_make_timestamp_ltz(make_date(2021, 07, 11), make_time(6, 30, 45.678), 'CET');
