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

-- Pre-epoch nanosecond values exercise the negative-epoch path; HOUR/MINUTE/SECOND
-- read the wall-clock fields in the session time zone.
SELECT hour(TIMESTAMP_LTZ '1960-01-01 13:24:35.123456789');
SELECT minute(TIMESTAMP_LTZ '1960-01-01 13:24:35.123456789');
SELECT second(TIMESTAMP_LTZ '1960-01-01 13:24:35.123456789');

-- LTZ nanos: the literal's time zone defines the instant, which is then extracted in the
-- session time zone (America/Los_Angeles, UTC-08:00). A source zone with a sub-hour offset
-- (Asia/Kolkata is UTC+05:30) shifts both the hour and the minute fields.
SELECT hour(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 Asia/Kolkata');
SELECT minute(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 Asia/Kolkata');
SELECT second(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 Asia/Kolkata');
SELECT hour(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 UTC');
SELECT minute(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 UTC');
SELECT second(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 UTC');

-- EXTRACT / date_part over nanosecond-precision values (SPARK-57340). HOUR and MINUTE are
-- equivalent to the hour()/minute() functions; SECOND keeps the sub-microsecond digits and
-- widens the result to DECIMAL(11, 9). LTZ extracts in the session time zone.
SELECT extract(HOUR FROM TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789');
SELECT extract(MINUTE FROM TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789');
SELECT extract(SECOND FROM TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789');
SELECT date_part('HOUR', TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789');
SELECT date_part('MINUTE', TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789');
SELECT date_part('SECOND', TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789');

-- Digits below the type's precision are floored at the type boundary, so they read back as
-- zeros in the DECIMAL(11, 9) result.
SELECT extract(SECOND FROM '2020-01-01 13:24:35.999999999' :: timestamp_ltz(7));
SELECT extract(SECOND FROM '2020-01-01 13:24:35.999999999' :: timestamp_ltz(8));
SELECT extract(SECOND FROM NULL :: timestamp_ltz(9));

-- Pre-epoch nanosecond values exercise the negative-epoch path.
SELECT extract(SECOND FROM TIMESTAMP_LTZ '1960-01-01 13:24:35.123456789');

-- A source zone with a sub-hour offset (Asia/Kolkata is UTC+05:30) shifts the minute field,
-- while the second field (including the nanosecond fraction) is offset-invariant.
SELECT extract(MINUTE FROM TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 Asia/Kolkata');
SELECT extract(SECOND FROM TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 Asia/Kolkata');

-- Date field functions over nanosecond-precision values (SPARK-57469). Date fields depend only
-- on the calendar date, so the precision, time-of-day and sub-microsecond digits never affect the
-- result; LTZ casts to DATE in the session time zone, so a zone shift can move the calendar day.
-- Columns are year, quarter, month, day, dayofyear, dayofweek (1=Sun..7=Sat),
-- weekday (0=Mon..6=Sun), weekofyear (ISO), yearofweek (ISO).
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_LTZ '2020-02-29 23:59:59.999999999') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_LTZ '1900-02-28 12:00:00.000000001') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_LTZ '2021-01-01 00:00:00.000000001') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_LTZ '2016-01-01 06:30:00.123456789') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_LTZ '2020-12-31 23:59:59.999999999') AS t(v);
-- Pre-epoch and far-past dates exercise the negative-epoch / minimum-date path.
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_LTZ '1960-07-15 06:07:08.123456789') AS t(v);
SELECT year(v), quarter(v), month(v), day(v), dayofyear(v), dayofweek(v), weekday(v),
       weekofyear(v), extract(YEAROFWEEK FROM v)
  FROM VALUES (TIMESTAMP_LTZ '0001-01-01 00:00:00.000000001') AS t(v);

-- Precision (7/8/9) and fraction invariance: the same instant read at different precisions and
-- fractions yields identical date fields.
SELECT year(v), month(v), day(v), dayofyear(v) FROM VALUES
  ('2020-02-29 13:24:35.000000001' :: timestamp_ltz(7)) AS t(v);
SELECT year(v), month(v), day(v), dayofyear(v) FROM VALUES
  ('2020-02-29 13:24:35.999999999' :: timestamp_ltz(8)) AS t(v);
SELECT year(v), month(v), day(v), dayofyear(v) FROM VALUES
  ('2020-02-29 13:24:35.000000000' :: timestamp_ltz(9)) AS t(v);

-- Time-zone-driven date shifts. An early-hours UTC instant rolls back a day in the session zone
-- (America/Los_Angeles, UTC-08:00), here crossing the year boundary to 2019-12-31.
SELECT year(v), month(v), day(v) FROM VALUES
  (TIMESTAMP_LTZ '2020-01-01 04:00:00.123456789 UTC') AS t(v);
SELECT year(v), month(v), day(v) FROM VALUES
  (TIMESTAMP_LTZ '2020-01-01 04:00:00.123456789') AS t(v);
-- A sub-hour-offset source zone (Asia/Kolkata, UTC+05:30) near the leap-day boundary.
SELECT year(v), month(v), day(v), dayofyear(v) FROM VALUES
  (TIMESTAMP_LTZ '2020-03-01 06:00:00.123456789 Asia/Kolkata') AS t(v);

-- EXTRACT / date_part date components (rewrite transitively to the same functions).
SELECT extract(YEAR FROM TIMESTAMP_LTZ '2020-02-29 12:00:00.123456789');
SELECT extract(MONTH FROM TIMESTAMP_LTZ '2020-02-29 12:00:00.123456789');
SELECT extract(DAY FROM TIMESTAMP_LTZ '2020-02-29 12:00:00.123456789');
SELECT extract(DOY FROM TIMESTAMP_LTZ '2020-02-29 12:00:00.123456789');
SELECT extract(WEEK FROM TIMESTAMP_LTZ '2021-01-01 12:00:00.123456789');
SELECT date_part('QUARTER', TIMESTAMP_LTZ '2020-04-01 00:00:00.000000001');
SELECT date_part('DOW', TIMESTAMP_LTZ '2020-02-29 00:00:00.000000001');
SELECT date_part('YEAROFWEEK', TIMESTAMP_LTZ '2021-01-01 00:00:00.000000001');

-- NULL nanosecond timestamp.
SELECT year(NULL :: timestamp_ltz(9)), month(NULL :: timestamp_ltz(9));

-- DATE <-> TIMESTAMP_LTZ(p) casts (SPARK-57323): midnight in the session zone / date extraction.
-- Nanosecond typed literals derive precision from the fractional digits (SPARK-57250).
SELECT DATE '2020-01-01'::timestamp_ltz(9);
SELECT DATE '2020-01-01'::timestamp_ltz(7);
SELECT TIMESTAMP_LTZ '2020-01-01 12:30:15.123456789'::date;
SELECT TIMESTAMP_LTZ '1960-01-01 00:00:00.000000001'::date;
-- Zone-dependence: a UTC instant in the early hours of Jan 1 falls on Dec 31 in the session
-- zone (America/Los_Angeles, UTC-08:00), so LTZ -> DATE yields the previous calendar day.
SELECT TIMESTAMP_LTZ '2020-01-01 04:00:00.123456789 UTC'::date;
-- Round trip date -> ltz(p) -> date.
SELECT DATE '2020-01-01'::timestamp_ltz(9)::date;
-- NULLs in both directions.
SELECT (NULL :: date) :: timestamp_ltz(9);
SELECT (NULL :: timestamp_ltz(9)) :: date;
-- DATE <-> nanos nested in complex types (array / map value / map key / struct field).
SELECT array(TIMESTAMP_LTZ '2020-01-01 12:30:15.123456789') :: array<date>;
SELECT array(DATE '2020-01-01') :: array<timestamp_ltz(9)>;
SELECT map('k', TIMESTAMP_LTZ '2020-01-01 12:30:15.123456789') :: map<string, date>;
SELECT map(DATE '2020-01-01', 'v') :: map<timestamp_ltz(9), string>;
SELECT named_struct('f', DATE '2020-01-01') :: struct<f: timestamp_ltz(9)>;

-- SPARK-57501: TIMESTAMP_LTZ(p) +/- ANSI day-time interval preserves nanos remainder.
SELECT TIMESTAMP_LTZ '2020-01-02 03:04:05.123456789 UTC' +
  INTERVAL '2 00:03:00.000456' DAY TO SECOND;
SELECT TIMESTAMP_LTZ '2020-01-02 03:04:05.123456789 UTC' -
  INTERVAL '1 00:04:00.000321' DAY TO SECOND;
SELECT TIMESTAMP_LTZ '1960-01-02 03:04:05.123456789 UTC' +
  INTERVAL '0 00:00:00.000001' DAY TO SECOND;
-- SPARK-57501: nanos timestamps support only ANSI day-time intervals. A (legacy) calendar interval
-- is rejected by TimestampAddInterval's type check, and a year-month interval has no supported
-- operator overload.
SELECT TIMESTAMP_LTZ '2020-01-02 03:04:05.123456789 UTC' + make_interval(0, 1, 0, 2, 0, 0, 0);
SELECT TIMESTAMP_LTZ '2020-01-02 03:04:05.123456789 UTC' + INTERVAL '1' MONTH;

-- SPARK-57103: MAX / MIN over nanosecond-precision TIMESTAMP_LTZ. The aggregate preserves the
-- nanosecond type and orders by the sub-microsecond remainder; NULLs are ignored. Values are
-- rendered in the session time zone (America/Los_Angeles).
SELECT max(c), min(c) FROM VALUES
  (TIMESTAMP_LTZ '2020-01-01 00:00:00.000000001 UTC'),
  (TIMESTAMP_LTZ '2020-01-01 00:00:00.000000999 UTC'),
  (CAST(NULL AS timestamp_ltz(9))) AS t(c);
-- GROUP BY a nanosecond key: two keys that share epochMicros but differ within the microsecond
-- must not collapse into one group.
SELECT c, count(*) FROM VALUES
  (TIMESTAMP_LTZ '2020-01-01 00:00:00.000000001 UTC'),
  (TIMESTAMP_LTZ '2020-01-01 00:00:00.000000999 UTC'),
  (TIMESTAMP_LTZ '2020-01-01 00:00:00.000000001 UTC') AS t(c)
  GROUP BY c ORDER BY c;

-- SPARK-57528: unix_timestamp / to_unix_timestamp over nanosecond-precision values. The result is
-- whole-second BIGINT; the sub-second digits are dropped. A literal without an explicit zone is
-- read in the session time zone (America/Los_Angeles, UTC-08:00); an explicit-zone literal fixes
-- the instant directly.
SELECT unix_timestamp(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789');
SELECT to_unix_timestamp(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789');
SELECT unix_timestamp(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 UTC');
SELECT to_unix_timestamp('2020-01-01 13:24:35.000000001 UTC' :: timestamp_ltz(9));
SELECT unix_timestamp('2020-01-01 13:24:35.999999999' :: timestamp_ltz(7));
-- Pre-epoch value exercises the negative-epoch path (truncation toward zero).
SELECT unix_timestamp(TIMESTAMP_LTZ '1969-12-31 23:59:59.500000000 UTC');
-- NULL nanosecond timestamp.
SELECT unix_timestamp(NULL :: timestamp_ltz(9)), to_unix_timestamp(NULL :: timestamp_ltz(9));

-- SPARK-57103: max_by / min_by return the nanosecond-precision TIMESTAMP_LTZ value at the extreme
-- ordering key, preserving the nanosecond type. The ordering keys are distinct so the result is
-- deterministic; a NULL-ordering row is ignored.
SELECT max_by(v, k), min_by(v, k) FROM VALUES
  (TIMESTAMP_LTZ '2020-01-01 00:00:00.000000001 UTC', 1),
  (TIMESTAMP_LTZ '2020-01-01 00:00:00.000000999 UTC', 3),
  (TIMESTAMP_LTZ '2020-01-01 00:00:00.000000500 UTC', 2),
  (TIMESTAMP_LTZ '2020-01-01 00:00:00.000000007 UTC', CAST(NULL AS INT)) AS t(v, k);

-- SPARK-57527: unix_nanos over nanosecond-precision values returns DECIMAL(21, 0) nanoseconds since
-- the epoch. The explicit-zone literals below fix the instant directly, independent of the session
-- time zone. The sub-microsecond digits are kept, truncated to the type's precision.
SELECT unix_nanos(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 UTC');
SELECT unix_nanos('2020-01-01 13:24:35.123456789 UTC' :: timestamp_ltz(7));
SELECT unix_nanos('2020-01-01 13:24:35.123456789 UTC' :: timestamp_ltz(8));
-- Far-future value: epochMicros * 1000 overflows a 64-bit BIGINT, exercising the DECIMAL path.
SELECT unix_nanos(TIMESTAMP_LTZ '9999-12-31 23:59:59.999999999 UTC');
-- Pre-epoch value exercises the negative-epoch path.
SELECT unix_nanos(TIMESTAMP_LTZ '1960-01-01 00:00:00.000000001 UTC');
-- NULL nanosecond timestamp.
SELECT unix_nanos(NULL :: timestamp_ltz(9));

-- SPARK-57526: timestamp_nanos builds a TIMESTAMP_LTZ(9) from a nanosecond count since the epoch.
-- An integral argument is accepted directly; the LTZ result renders in the session zone.
SELECT timestamp_nanos(1230219000123456789);
-- Negative input floors toward the past, so the sub-microsecond remainder stays in [0, 999].
SELECT timestamp_nanos(-1);
-- DECIMAL input reaches beyond a 64-bit BIGINT, up to year 9999 (nanos ~ 2.5e20).
SELECT timestamp_nanos(253402300799999999999BD);
-- Out-of-range input: epochMicros overflows a 64-bit long, so the conversion fails at runtime.
SELECT timestamp_nanos(10000000000000000000000000BD);
-- DOUBLE is rejected at analysis: only integral and DECIMAL nanosecond counts are accepted.
SELECT timestamp_nanos(1.0D);
-- NULL input.
SELECT timestamp_nanos(CAST(NULL AS BIGINT));

-- SPARK-57454: implicit type coercion / widening over nanosecond TIMESTAMP_LTZ(p). The resolved
-- common type itself is unit-tested in TypeCoercionSuite / AnsiTypeCoercionSuite, and the operator
-- wiring (schema and boolean outcomes for UNION/coalesce/CASE/IN/comparison) in
-- TimestampNanosWideningSuite; the cases below complement those by locking the resolved type with
-- typeof() and the end-to-end rendered values, by covering operators those suites do not
-- (greatest/least and the array/map constructors), and by exercising the mixed time-zone family
-- rule that has no TIMESTAMP_NTZ counterpart. Values span the min/max supported instants, the 1582
-- Julian/Gregorian boundary (proleptic Gregorian), pre/post epoch, near-current values, varied
-- fractions / precisions, and non-standard source zones. Bare literals are interpreted in the
-- session zone (America/Los_Angeles) and so round-trip on rendering; an explicit source zone
-- (e.g. the sub-hour offsets Asia/Kolkata +05:30 and Asia/Kathmandu +05:45) shifts the rendered
-- wall clock deterministically.

-- UNION ALL widens micro -> nanos: the minimum and maximum supported instants.
SELECT typeof(c), c FROM (
    SELECT TIMESTAMP_LTZ '0001-01-01 00:00:00' AS c
    UNION ALL SELECT TIMESTAMP_LTZ '9999-12-31 23:59:59.999999999') ORDER BY c;
-- UNION ALL widens nanos(7)/nanos(9) -> nanos(9): around the 1582 Julian/Gregorian boundary.
SELECT typeof(c), c FROM (
    SELECT '1582-10-04 12:30:45.1234567' :: timestamp_ltz(7) AS c
    UNION ALL SELECT '1582-10-15 23:59:59.123456789' :: timestamp_ltz(9)) ORDER BY c;

-- coalesce keeps the first non-null, widened: pre-epoch boundary read from a +05:30-offset zone.
SELECT typeof(v), v FROM (SELECT coalesce(
    '1969-12-31 23:59:59.0000001 Asia/Kolkata' :: timestamp_ltz(7),
    '1969-12-31 23:59:59.999999999 UTC' :: timestamp_ltz(9)) AS v);
-- CASE WHEN unifies its branches: a near-current value read from a +05:45-offset zone.
SELECT typeof(v), v FROM (SELECT CASE WHEN true
    THEN TIMESTAMP_LTZ '2026-06-21 10:16:30 Asia/Kathmandu'
    ELSE '2026-06-21 10:16:30.987654321 UTC' :: timestamp_ltz(9) END AS v);

-- nanos <-> DATE widening: the minimum DATE adopts the nanos family, midnight in the session zone.
SELECT typeof(v), v FROM (SELECT coalesce(
    DATE '0001-01-01', '2020-01-01 00:00:00.12345678' :: timestamp_ltz(8)) AS v);

-- greatest / least widen their arguments to the common nanosecond type and pick the extreme instant.
SELECT typeof(greatest(TIMESTAMP_LTZ '0001-01-01 00:00:00',
    '9999-12-31 23:59:59.999999999' :: timestamp_ltz(9)));
SELECT greatest(TIMESTAMP_LTZ '1500-03-01 12:00:00',
    '1582-10-15 00:00:00.123456789' :: timestamp_ltz(9),
    TIMESTAMP_LTZ '2026-06-21 10:16:30.5');
SELECT least('1970-01-01 00:00:00.0000001' :: timestamp_ltz(7),
    '1969-12-31 23:59:59.999999999' :: timestamp_ltz(9));

-- array() unifies element types and map() value types: a spread of eras, zones and precisions.
SELECT array('0001-01-01 00:00:00.0000001' :: timestamp_ltz(7),
    TIMESTAMP_LTZ '2026-06-21 10:16:30 Asia/Kolkata',
    '9999-12-31 23:59:59.999999999' :: timestamp_ltz(9));
SELECT typeof(array(TIMESTAMP_LTZ '9999-12-31 23:59:59',
    '0001-01-01 00:00:00.000000001' :: timestamp_ltz(9)));
SELECT map('min', '0001-01-01 00:00:00.000000001' :: timestamp_ltz(9),
    'max', TIMESTAMP_LTZ '9999-12-31 23:59:59.999999');

-- Mixed time-zone families widen to the LTZ family (mirrors TIMESTAMP + TIMESTAMP_NTZ -> TIMESTAMP).
-- A value-pinned case: the inserted cross-family cast reinterprets the NTZ wall clock as an instant
-- in the session zone (America/Los_Angeles) and the result is rendered back there, so it round-trips
-- to the same wall clock with the sub-microsecond digits preserved. This locks the cast's
-- sessionLocalTimeZone wiring -- a UTC misread would render a different instant.
SELECT typeof(v), v FROM (SELECT coalesce(
    TIMESTAMP_NTZ '2026-06-21 10:16:30.123456789',
    '1970-01-01 00:00:00.000000001 UTC' :: timestamp_ltz(9)) AS v);
-- The remaining mixed-family cases assert the resolved type only (varied precisions and eras).
SELECT typeof(c) FROM (
    SELECT TIMESTAMP_NTZ '1582-10-15 00:00:00' AS c
    UNION ALL SELECT '9999-12-31 23:59:59.999999999' :: timestamp_ltz(9));
SELECT typeof(coalesce('0001-01-01 00:00:00.0000001' :: timestamp_ntz(7),
    '2026-06-21 10:16:30.123456789 UTC' :: timestamp_ltz(9)));
SELECT typeof(CASE WHEN true
    THEN '1969-12-31 23:59:59.1234567' :: timestamp_ntz(7)
    ELSE '1970-01-01 00:00:00.123456789 UTC' :: timestamp_ltz(9) END);

-- SORT / ORDER BY tie-breaks on the sub-microsecond remainder: 001 and 999 share a microsecond,
-- 1000 rolls into the next, so a micro-truncating sort would misorder them (full value 001<999<1000).
SELECT v FROM (
    SELECT TIMESTAMP_LTZ '2020-01-01 00:00:00.000001000' AS v
    UNION ALL SELECT TIMESTAMP_LTZ '2020-01-01 00:00:00.000000999'
    UNION ALL SELECT TIMESTAMP_LTZ '2020-01-01 00:00:00.000000001') ORDER BY v;

-- row_number() over a nanosecond ORDER BY key: the row numbers follow the sub-microsecond order.
SELECT v, row_number() OVER (ORDER BY v) AS rn FROM (
    SELECT TIMESTAMP_LTZ '2020-01-01 00:00:00.000000900' AS v
    UNION ALL SELECT TIMESTAMP_LTZ '2020-01-01 00:00:00.000000100'
    UNION ALL SELECT TIMESTAMP_LTZ '2020-01-01 00:00:00.000000500') ORDER BY rn;

-- lead() over a nanosecond ORDER BY key returns the next sub-microsecond value (carrier round-trip).
SELECT v, lead(v) OVER (ORDER BY v) AS next_v FROM (
    SELECT TIMESTAMP_LTZ '2020-01-01 00:00:00.000000900' AS v
    UNION ALL SELECT TIMESTAMP_LTZ '2020-01-01 00:00:00.000000100'
    UNION ALL SELECT TIMESTAMP_LTZ '2020-01-01 00:00:00.000000500') ORDER BY v;

-- SPARK-57814: unix_seconds / unix_millis / unix_micros over nanosecond-precision values. The result
-- is a whole BIGINT count of the unit; sub-unit digits are dropped. Explicit-zone literals fix the
-- instant directly, independent of the session time zone.
SELECT unix_seconds(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 UTC');
SELECT unix_millis(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 UTC');
SELECT unix_micros(TIMESTAMP_LTZ '2020-01-01 13:24:35.123456789 UTC');
SELECT unix_micros('2020-01-01 13:24:35.999999999 UTC' :: timestamp_ltz(7));
-- Pre-epoch value: floorDiv floors toward -inf, so unix_seconds -> -1 (not 0).
SELECT unix_seconds(TIMESTAMP_LTZ '1969-12-31 23:59:59.500000000 UTC');
-- NULL nanosecond timestamp.
SELECT unix_seconds(NULL :: timestamp_ltz(9)), unix_millis(NULL :: timestamp_ltz(9)), unix_micros(NULL :: timestamp_ltz(9));

-- SPARK-57821: date_trunc keeps the nanosecond type/family and zeroes the whole fraction (including
-- the sub-microsecond digits); MICROSECOND keeps epochMicros and only drops nanosWithinMicro. LTZ
-- truncates in the session time zone (America/Los_Angeles, UTC-08:00), so DAY over an early-hours
-- UTC instant floors to the previous calendar day in the session zone.
SELECT date_trunc('SECOND', TIMESTAMP_LTZ '2020-01-01 12:34:56.123456789 UTC');
SELECT date_trunc('MICROSECOND', TIMESTAMP_LTZ '2020-01-01 12:34:56.123456789 UTC');
SELECT date_trunc('HOUR', '2020-01-01 12:34:56.123456789 UTC' :: timestamp_ltz(9));
SELECT date_trunc('DAY', '2020-01-01 04:00:00.000000123 UTC' :: timestamp_ltz(7));
-- An unsupported (sub-microsecond) unit yields NULL; the result still carries the nanos type.
SELECT date_trunc('NANOSECOND', TIMESTAMP_LTZ '2020-01-01 12:34:56.123456789 UTC');
