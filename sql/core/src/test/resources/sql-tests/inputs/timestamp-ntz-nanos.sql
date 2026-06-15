-- Nanosecond-precision TIMESTAMP_NTZ(p) (p in [7, 9]) in Hive results (SPARK-57257).
-- NTZ values are zone-independent.

--SET spark.sql.timestampNanosTypes.enabled=true
--SET spark.sql.session.timeZone=America/Los_Angeles

-- Precision-driven fraction width: sub-p digits are floored.
SELECT CAST('2020-01-01 00:00:00.123456789' AS timestamp_ntz(7));
SELECT CAST('2020-01-01 00:00:00.123456789' AS timestamp_ntz(8));
SELECT CAST('2020-01-01 00:00:00.123456789' AS timestamp_ntz(9));

-- Trailing-zero trimming: an all-zero fraction renders as no fraction at all.
SELECT CAST('2020-01-01 00:00:00.999999000' AS timestamp_ntz(9));
SELECT CAST('2020-01-01 00:00:00.000000999' AS timestamp_ntz(9));
SELECT CAST('2020-01-01 00:00:00.000000001' AS timestamp_ntz(9));
SELECT CAST('2020-01-01 00:00:00.000000001' AS timestamp_ntz(8));
SELECT CAST('2020-01-01 00:00:00.000000001' AS timestamp_ntz(7));

-- Pre-1970 values exercise the negative-epoch path.
SELECT CAST('1960-01-01 00:00:00.000000001' AS timestamp_ntz(9));
SELECT CAST('1960-01-01 00:00:00.123456789' AS timestamp_ntz(7));

-- Nested values (array / map / struct).
SELECT array(CAST('2020-01-01 00:00:00.123456789' AS timestamp_ntz(9)));
SELECT map('k', CAST('2020-01-01 00:00:00.123456789' AS timestamp_ntz(9)));
SELECT named_struct('f', CAST('2020-01-01 00:00:00.123456789' AS timestamp_ntz(9)));

-- NULL values (top-level and nested).
SELECT CAST(NULL AS timestamp_ntz(9));
SELECT array(CAST(NULL AS timestamp_ntz(9)));
SELECT map('k', CAST(NULL AS timestamp_ntz(9)));
SELECT named_struct('f', CAST(NULL AS timestamp_ntz(9)));

-- HOUR/MINUTE/SECOND over nanosecond-precision values (SPARK-57315). NTZ extracts the
-- wall-clock components, so the result is zone-independent and the sub-microsecond digits
-- never affect the integer field.
SELECT hour(TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT minute(TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT second(TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT hour('2020-01-01 13:24:35.999999999' :: timestamp_ntz(7));
SELECT second('2020-01-01 13:24:35.999999999' :: timestamp_ntz(8));
SELECT hour(NULL :: timestamp_ntz(9));

-- Pre-epoch nanosecond values exercise the negative-epoch path; HOUR/MINUTE/SECOND
-- still read the wall-clock fields and remain zone-independent.
SELECT hour(TIMESTAMP_NTZ '1960-01-01 13:24:35.123456789');
SELECT minute(TIMESTAMP_NTZ '1960-01-01 13:24:35.123456789');
SELECT second(TIMESTAMP_NTZ '1960-01-01 13:24:35.123456789');

-- EXTRACT / date_part over nanosecond-precision values (SPARK-57340). HOUR and MINUTE are
-- equivalent to the hour()/minute() functions; SECOND keeps the sub-microsecond digits and
-- widens the result to DECIMAL(11, 9).
SELECT extract(HOUR FROM TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT extract(MINUTE FROM TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT extract(SECOND FROM TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT date_part('HOUR', TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT date_part('MINUTE', TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT date_part('SECOND', TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');

-- Digits below the type's precision are floored at the type boundary, so they read back as
-- zeros in the DECIMAL(11, 9) result.
SELECT extract(SECOND FROM '2020-01-01 13:24:35.999999999' :: timestamp_ntz(7));
SELECT extract(SECOND FROM '2020-01-01 13:24:35.999999999' :: timestamp_ntz(8));
SELECT extract(SECOND FROM NULL :: timestamp_ntz(9));

-- Pre-epoch nanosecond values exercise the negative-epoch path.
SELECT extract(SECOND FROM TIMESTAMP_NTZ '1960-01-01 13:24:35.123456789');

-- DATE <-> TIMESTAMP_NTZ(p) casts (SPARK-57323): midnight UTC / date extraction (zone-independent).
-- Nanosecond typed literals derive precision from the fractional digits (SPARK-57250).
SELECT DATE '2020-01-01'::timestamp_ntz(9);
SELECT DATE '2020-01-01'::timestamp_ntz(7);
SELECT TIMESTAMP_NTZ '2020-01-01 12:30:15.123456789'::date;
SELECT TIMESTAMP_NTZ '1960-01-01 00:00:00.000000001'::date;
-- Round trip date -> ntz(p) -> date.
SELECT DATE '2020-01-01'::timestamp_ntz(9)::date;
-- NULLs in both directions.
SELECT (NULL :: date) :: timestamp_ntz(9);
SELECT (NULL :: timestamp_ntz(9)) :: date;
-- DATE <-> nanos nested in complex types (array / map value / map key / struct field).
SELECT array(TIMESTAMP_NTZ '2020-01-01 12:30:15.123456789') :: array<date>;
SELECT array(DATE '2020-01-01') :: array<timestamp_ntz(9)>;
SELECT map('k', TIMESTAMP_NTZ '2020-01-01 12:30:15.123456789') :: map<string, date>;
SELECT map(DATE '2020-01-01', 'v') :: map<timestamp_ntz(9), string>;
SELECT named_struct('f', DATE '2020-01-01') :: struct<f: timestamp_ntz(9)>;
