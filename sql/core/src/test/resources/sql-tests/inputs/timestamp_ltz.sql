-- timestamp_ltz literals and constructors
--CONFIG_DIM1 spark.sql.timestampType=TIMESTAMP_LTZ
--CONFIG_DIM1 spark.sql.timestampType=TIMESTAMP_NTZ

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

-- [SPARK-31710] TIMESTAMP_SECONDS, TIMESTAMP_MILLISECONDS and TIMESTAMP_MICROSECONDS that always create timestamp_ltz
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
