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

-- Date field functions over nanosecond-precision values (SPARK-57469). Date fields depend only
-- on the calendar date, so the precision, time-of-day and sub-microsecond digits never affect the
-- result; the values below exercise leap years, ISO-week and quarter boundaries, pre-epoch and
-- far-past dates, and varied precisions / fractions. Columns are year, quarter, month, day,
-- dayofyear, dayofweek (1=Sun..7=Sat), weekday (0=Mon..6=Sun), weekofyear (ISO), yearofweek (ISO).
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_NTZ '2020-02-29 23:59:59.999999999') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_NTZ '1900-02-28 12:00:00.000000001') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_NTZ '2021-01-01 00:00:00.000000001') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_NTZ '2016-01-01 06:30:00.123456789') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_NTZ '2020-03-31 13:24:35.123456789') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_NTZ '2020-04-01 00:00:00.000000001') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_NTZ '2020-12-31 23:59:59.999999999') AS t(v);
-- Pre-epoch and far-past dates exercise the negative-epoch / minimum-date path.
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_NTZ '1960-07-15 06:07:08.123456789') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_NTZ '0001-01-01 00:00:00.000000001') AS t(v);

-- Precision (7/8/9) and fraction invariance: the same date read at different precisions and
-- fractions yields identical date fields.
SELECT year(v), month(v), day(v), dayofyear(v) FROM VALUES
  ('2020-02-29 13:24:35.000000001' :: timestamp_ntz(7)) AS t(v);
SELECT year(v), month(v), day(v), dayofyear(v) FROM VALUES
  ('2020-02-29 13:24:35.999999999' :: timestamp_ntz(8)) AS t(v);
SELECT year(v), month(v), day(v), dayofyear(v) FROM VALUES
  ('2020-02-29 13:24:35.000000000' :: timestamp_ntz(9)) AS t(v);

-- EXTRACT / date_part date components (rewrite transitively to the same functions).
SELECT extract(YEAR FROM TIMESTAMP_NTZ '2020-02-29 12:00:00.123456789');
SELECT extract(MONTH FROM TIMESTAMP_NTZ '2020-02-29 12:00:00.123456789');
SELECT extract(DAY FROM TIMESTAMP_NTZ '2020-02-29 12:00:00.123456789');
SELECT extract(DOY FROM TIMESTAMP_NTZ '2020-02-29 12:00:00.123456789');
SELECT extract(WEEK FROM TIMESTAMP_NTZ '2021-01-01 12:00:00.123456789');
SELECT date_part('QUARTER', TIMESTAMP_NTZ '2020-04-01 00:00:00.000000001');
SELECT date_part('DOW', TIMESTAMP_NTZ '2020-02-29 00:00:00.000000001');
SELECT date_part('YEAROFWEEK', TIMESTAMP_NTZ '2021-01-01 00:00:00.000000001');

-- NULL nanosecond timestamp.
SELECT year(NULL :: timestamp_ntz(9)), month(NULL :: timestamp_ntz(9));

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

-- SPARK-57501: TIMESTAMP_NTZ(p) +/- ANSI day-time interval preserves nanos remainder.
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' + INTERVAL '2 00:03:00.000456' DAY TO SECOND;
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' - INTERVAL '1 00:04:00.000321' DAY TO SECOND;
SELECT TIMESTAMP_NTZ '1960-01-02 03:04:05.123456789' + INTERVAL '0 00:00:00.000001' DAY TO SECOND;
-- SPARK-57501: nanos timestamps support only ANSI day-time intervals. A (legacy) calendar interval
-- is rejected by TimestampAddInterval's type check, and a year-month interval has no supported
-- operator overload.
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' + make_interval(0, 1, 0, 2, 0, 0, 0);
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' + INTERVAL '1' MONTH;

-- SPARK-57818: convert_timezone over nanosecond-precision TIMESTAMP_NTZ. The sub-microsecond
-- remainder is carried through unchanged; only the whole-microsecond part shifts with the zone
-- offset, and the result keeps the source's exact precision.
SELECT convert_timezone('Europe/Brussels', 'Europe/Moscow',
    TIMESTAMP_NTZ '2022-03-27 03:00:00.123456789');
SELECT typeof(convert_timezone('Europe/Brussels', 'Europe/Moscow',
    '2022-03-27 03:00:00.1234567' :: timestamp_ntz(7)));
-- NULL nanosecond timestamp.
SELECT convert_timezone('America/Los_Angeles', 'UTC', CAST(NULL AS timestamp_ntz(9)));
-- convert_timezone is NTZ-only; a nanosecond LTZ(p) source is rejected rather than silently
-- reinterpreted, unlike the microsecond path, which does implicit-cast a plain LTZ TimestampType
-- argument down to TIMESTAMP_NTZ.
SELECT convert_timezone('Europe/Brussels', 'Europe/Moscow',
    '2022-03-27 03:00:00.123456789 UTC' :: timestamp_ltz(9));

-- SPARK-57103: MAX / MIN over nanosecond-precision TIMESTAMP_NTZ. The aggregate preserves the
-- nanosecond type and orders by the sub-microsecond remainder (two values share the same
-- microsecond and differ only within it); NULLs are ignored.
SELECT max(c), min(c) FROM VALUES
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'),
  (CAST(NULL AS timestamp_ntz(9))) AS t(c);
-- GROUP BY a nanosecond key: two keys that share epochMicros but differ within the microsecond
-- must not collapse into one group.
SELECT c, count(*) FROM VALUES
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001') AS t(c)
  GROUP BY c ORDER BY c;

-- SPARK-57528: unix_timestamp / to_unix_timestamp over nanosecond-precision values. The result is
-- whole-second BIGINT; the sub-second digits are dropped and NTZ applies no zone shift, so the
-- wall-clock value is read as the epoch instant.
SELECT unix_timestamp(TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT to_unix_timestamp(TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT unix_timestamp('2020-01-01 13:24:35.999999999' :: timestamp_ntz(7));
SELECT to_unix_timestamp('2020-01-01 13:24:35.000000001' :: timestamp_ntz(9));
-- Pre-epoch value exercises the negative-epoch path (truncation toward zero).
SELECT unix_timestamp(TIMESTAMP_NTZ '1969-12-31 23:59:59.500000000');
-- NULL nanosecond timestamp.
SELECT unix_timestamp(NULL :: timestamp_ntz(9)), to_unix_timestamp(NULL :: timestamp_ntz(9));

-- SPARK-57103: max_by / min_by return the nanosecond-precision TIMESTAMP_NTZ value at the extreme
-- ordering key, preserving the nanosecond type. The ordering keys are distinct so the result is
-- deterministic; a NULL-ordering row is ignored.
SELECT max_by(v, k), min_by(v, k) FROM VALUES
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001', 1),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999', 3),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000500', 2),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000007', CAST(NULL AS INT)) AS t(v, k);

-- SPARK-57527: unix_nanos over nanosecond-precision values returns DECIMAL(21, 0) nanoseconds since
-- the epoch; NTZ applies no zone shift, so the wall-clock value is read as the epoch instant. The
-- sub-microsecond digits are kept, truncated to the type's precision.
SELECT unix_nanos(TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT unix_nanos('2020-01-01 13:24:35.123456789' :: timestamp_ntz(7));
SELECT unix_nanos('2020-01-01 13:24:35.123456789' :: timestamp_ntz(8));
-- Far-future value: epochMicros * 1000 overflows a 64-bit BIGINT, exercising the DECIMAL path.
SELECT unix_nanos(TIMESTAMP_NTZ '9999-12-31 23:59:59.999999999');
-- Pre-epoch value exercises the negative-epoch path.
SELECT unix_nanos(TIMESTAMP_NTZ '1960-01-01 00:00:00.000000001');
-- NULL nanosecond timestamp.
SELECT unix_nanos(NULL :: timestamp_ntz(9));

-- SPARK-57454: implicit type coercion / widening over nanosecond TIMESTAMP_NTZ(p). The resolved
-- common type itself is unit-tested in TypeCoercionSuite / AnsiTypeCoercionSuite, and the operator
-- wiring (schema and boolean outcomes for UNION/coalesce/CASE/IN/comparison) in
-- TimestampNanosWideningSuite; the cases below complement those by locking the resolved type with
-- typeof() and the end-to-end rendered values, by covering operators those suites do not
-- (greatest/least and the array/map constructors), and by spanning the value range: the min/max
-- supported timestamps, the 1582 Julian/Gregorian boundary (Spark uses the proleptic Gregorian
-- calendar), pre/post epoch, near-current values, and varied fractions / precisions. NTZ is
-- zone-independent, so the time-zone dimension is exercised in timestamp-ltz-nanos.sql instead.

-- UNION ALL widens micro -> nanos: the minimum and maximum supported TIMESTAMP_NTZ values.
SELECT typeof(c), c FROM (
    SELECT TIMESTAMP_NTZ '0001-01-01 00:00:00' AS c
    UNION ALL SELECT TIMESTAMP_NTZ '9999-12-31 23:59:59.999999999') ORDER BY c;
-- UNION ALL widens nanos(7)/nanos(9) -> nanos(9): around the 1582 Julian/Gregorian boundary
-- (1582-10-05..14 are valid dates only under the proleptic Gregorian calendar).
SELECT typeof(c), c FROM (
    SELECT '1582-10-04 12:30:45.1234567' :: timestamp_ntz(7) AS c
    UNION ALL SELECT '1582-10-15 23:59:59.123456789' :: timestamp_ntz(9)) ORDER BY c;

-- coalesce keeps the first non-null, widened to the wider precision: pre-epoch boundary values.
SELECT typeof(v), v FROM (SELECT coalesce(
    '1969-12-31 23:59:59.0000001' :: timestamp_ntz(7),
    '1969-12-31 23:59:59.999999999' :: timestamp_ntz(9)) AS v);
-- CASE WHEN unifies its branches: a near-current value taken from the micro branch.
SELECT typeof(v), v FROM (SELECT CASE WHEN true
    THEN TIMESTAMP_NTZ '2026-06-21 10:16:30'
    ELSE '2026-06-21 10:16:30.987654321' :: timestamp_ntz(9) END AS v);

-- nanos <-> DATE widening: the minimum DATE adopts the nanos family and renders at midnight.
SELECT typeof(v), v FROM (SELECT coalesce(
    DATE '0001-01-01', '2020-01-01 00:00:00.12345678' :: timestamp_ntz(8)) AS v);

-- greatest / least widen their arguments to the common nanosecond type and pick the extreme instant.
SELECT typeof(greatest(TIMESTAMP_NTZ '0001-01-01 00:00:00',
    '9999-12-31 23:59:59.999999999' :: timestamp_ntz(9)));
SELECT greatest(TIMESTAMP_NTZ '1500-03-01 12:00:00',
    '1582-10-15 00:00:00.123456789' :: timestamp_ntz(9),
    TIMESTAMP_NTZ '2026-06-21 10:16:30.5');
SELECT least('1970-01-01 00:00:00.0000001' :: timestamp_ntz(7),
    '1969-12-31 23:59:59.999999999' :: timestamp_ntz(9));

-- array() unifies element types and map() value types: a spread of eras, fractions and precisions.
SELECT array('0001-01-01 00:00:00.0000001' :: timestamp_ntz(7),
    TIMESTAMP_NTZ '2026-06-21 10:16:30',
    '9999-12-31 23:59:59.999999999' :: timestamp_ntz(9));
SELECT typeof(array(TIMESTAMP_NTZ '9999-12-31 23:59:59',
    '0001-01-01 00:00:00.000000001' :: timestamp_ntz(9)));
SELECT map('min', '0001-01-01 00:00:00.000000001' :: timestamp_ntz(9),
    'max', TIMESTAMP_NTZ '9999-12-31 23:59:59.999999');

-- SORT / ORDER BY tie-breaks on the sub-microsecond remainder: 001 and 999 share a microsecond,
-- 1000 rolls into the next, so a micro-truncating sort would misorder them (full value 001<999<1000).
SELECT v FROM (
    SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000001000' AS v
    UNION ALL SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'
    UNION ALL SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001') ORDER BY v;

-- row_number() over a nanosecond ORDER BY key: the row numbers follow the sub-microsecond order.
SELECT v, row_number() OVER (ORDER BY v) AS rn FROM (
    SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000900' AS v
    UNION ALL SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000100'
    UNION ALL SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000500') ORDER BY rn;

-- lead() over a nanosecond ORDER BY key returns the next sub-microsecond value (carrier round-trip).
SELECT v, lead(v) OVER (ORDER BY v) AS next_v FROM (
    SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000900' AS v
    UNION ALL SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000100'
    UNION ALL SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000500') ORDER BY v;
