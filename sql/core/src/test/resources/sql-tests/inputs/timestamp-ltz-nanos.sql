-- Nanosecond-precision TIMESTAMP_LTZ(p) (p in [7, 9]) in Hive results (SPARK-57257).
-- LTZ values are rendered in the session time zone.

--SET spark.sql.timestampNanosTypes.enabled=true
--SET spark.sql.session.timeZone=America/Los_Angeles

-- Precision-driven fraction width: sub-p digits are floored.
SELECT CAST('2020-01-01 00:00:00.123456789' AS timestamp_ltz(7));
SELECT CAST('2020-01-01 00:00:00.123456789' AS timestamp_ltz(8));
SELECT CAST('2020-01-01 00:00:00.123456789' AS timestamp_ltz(9));

-- Trailing-zero trimming: an all-zero fraction renders as no fraction at all.
SELECT CAST('2020-01-01 00:00:00.999999000' AS timestamp_ltz(9));
SELECT CAST('2020-01-01 00:00:00.000000999' AS timestamp_ltz(9));
SELECT CAST('2020-01-01 00:00:00.000000001' AS timestamp_ltz(9));
SELECT CAST('2020-01-01 00:00:00.000000001' AS timestamp_ltz(8));
SELECT CAST('2020-01-01 00:00:00.000000001' AS timestamp_ltz(7));

-- Nested values (array / map / struct).
SELECT array(CAST('2020-01-01 00:00:00.123456789' AS timestamp_ltz(9)));
SELECT map('k', CAST('2020-01-01 00:00:00.123456789' AS timestamp_ltz(9)));
SELECT named_struct('f', CAST('2020-01-01 00:00:00.123456789' AS timestamp_ltz(9)));
