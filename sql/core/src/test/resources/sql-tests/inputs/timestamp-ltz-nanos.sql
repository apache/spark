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

-- Pre-1970 values exercise the negative-epoch path.
SELECT CAST('1960-01-01 00:00:00.000000001' AS timestamp_ltz(9));
SELECT CAST('1960-01-01 00:00:00.123456789' AS timestamp_ltz(7));

-- Nested values (array / map / struct).
SELECT array(CAST('2020-01-01 00:00:00.123456789' AS timestamp_ltz(9)));
SELECT map('k', CAST('2020-01-01 00:00:00.123456789' AS timestamp_ltz(9)));
SELECT named_struct('f', CAST('2020-01-01 00:00:00.123456789' AS timestamp_ltz(9)));

-- NULL values (top-level and nested).
SELECT CAST(NULL AS timestamp_ltz(9));
SELECT array(CAST(NULL AS timestamp_ltz(9)));
SELECT map('k', CAST(NULL AS timestamp_ltz(9)));
SELECT named_struct('f', CAST(NULL AS timestamp_ltz(9)));

-- HOUR/MINUTE/SECOND over nanosecond-precision values (SPARK-57315). LTZ extracts in the
-- session time zone; the sub-microsecond digits never affect the integer field.
SELECT hour(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789');
SELECT minute(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789');
SELECT second(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789');
SELECT hour('2020-01-01 13:24:35.999999999' :: timestamp_ltz(7));
SELECT second('2020-01-01 13:24:35.999999999' :: timestamp_ltz(8));
SELECT hour(NULL :: timestamp_ltz(9));
