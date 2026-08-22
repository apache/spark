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
-- SPARK-57825: TIMESTAMP_NTZ(p) +/- ANSI year-month interval keeps the nanos type/precision and
-- carries the whole fraction (including the sub-microsecond digits) through unchanged; a month
-- shift never touches the time of day.
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' + INTERVAL '1' YEAR;
-- The interval-first operand order resolves to the same addition.
SELECT INTERVAL '1' YEAR + TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789';
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' + INTERVAL '1' MONTH;
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' - INTERVAL '1-2' YEAR TO MONTH;
-- Jan-31 -> Feb-29 day clamp on a pre-epoch (leap-year) value.
SELECT TIMESTAMP_NTZ '1960-01-31 03:04:05.123456789' + INTERVAL '1' MONTH;
-- SPARK-57501, SPARK-57825: nanos timestamps support ANSI day-time and year-month intervals; the
-- legacy calendar interval is still rejected by TimestampAddInterval's type check.
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' + make_interval(0, 1, 0, 2, 0, 0, 0);

-- SPARK-57832: TIMESTAMP_NTZ(p) - TIMESTAMP_NTZ(p) yields a microsecond-grid DayTimeIntervalType.
-- Only each operand's epochMicros participates, so the sub-microsecond remainder is truncated: the
-- 789/111 sub-micro digits drop out and the difference is exactly 1 day + 0.123456 s.
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' - TIMESTAMP_NTZ '2020-01-01 03:04:05.000000111';
-- Two values inside the same microsecond subtract to zero once the remainder is truncated.
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' - TIMESTAMP_NTZ '2020-01-02 03:04:05.123456001';
-- The subtraction is antisymmetric.
SELECT TIMESTAMP_NTZ '2020-01-01 03:04:05.000000111' - TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789';
-- Mixed precision (7 vs 9) widens to the common nanos type before subtracting; the result stays on
-- the micros grid.
SELECT ('2020-01-02 03:04:05.1234567' :: timestamp_ntz(7)) - ('2020-01-01 03:04:05.000000009' :: timestamp_ntz(9));
-- Mixed with a micro TIMESTAMP_NTZ operand.
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' - TIMESTAMP_NTZ '2020-01-02 03:04:05';
-- A DATE operand is cast to the nanos type (midnight); the fraction below the micro grid drops.
SELECT TIMESTAMP_NTZ '2020-01-02 00:00:00.000000789' - DATE '2020-01-01';
-- Pre-epoch operand exercises the negative-epoch path.
SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.123456789' - TIMESTAMP_NTZ '1960-01-01 00:00:00.000000999';
-- NULL operand propagates.
SELECT TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789' - CAST(NULL AS timestamp_ntz(9));

-- SPARK-57818: convert_timezone over nanosecond-precision TIMESTAMP_NTZ. The sub-microsecond
-- remainder is carried through unchanged; only the whole-microsecond part shifts with the zone
-- offset, and the result keeps the source's exact precision.
SELECT convert_timezone('Europe/Brussels', 'Europe/Moscow',
    TIMESTAMP_NTZ '2022-03-27 03:00:00.123456789');
SELECT typeof(convert_timezone('Europe/Brussels', 'Europe/Moscow',
    '2022-03-27 03:00:00.1234567' :: timestamp_ntz(7)));
-- NULL nanosecond timestamp.
SELECT convert_timezone('America/Los_Angeles', 'UTC', CAST(NULL AS timestamp_ntz(9)));

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
-- GROUP BY a nanosecond key with aggregates and a NULL group: exact-duplicate keys collapse, two
-- keys sharing epochMicros but differing within the microsecond stay in separate groups, and all
-- NULL keys group together (unlike an equi-join). Three groups: .000000001 (count 2, sum 3),
-- .000000999 (count 1, sum 3), NULL (count 2, sum 9).
SELECT k, count(*), sum(v) FROM VALUES
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001', 1),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001', 2),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999', 3),
  (CAST(NULL AS timestamp_ntz(9)), 4),
  (CAST(NULL AS timestamp_ntz(9)), 5) AS t(k, v)
  GROUP BY k ORDER BY k;

-- SPARK-56822: mode over nanosecond-precision TIMESTAMP_NTZ. Frequencies are counted on the full
-- nanos value, so the most-frequent value is selected down to the sub-microsecond and the result
-- type stays TIMESTAMP_NTZ(9). .000000001 appears twice, .000000999 once.
SELECT mode(c) FROM VALUES
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001') AS t(c);

-- SPARK-56822: collect_set over nanosecond-precision TIMESTAMP_NTZ. It deduplicates on the full
-- sub-microsecond value: the two .000000001 rows collapse to one, the .000000999 row stays, so the
-- sorted set has two distinct elements and the element type stays TIMESTAMP_NTZ(9). collect_set
-- order is non-deterministic, so the output is stabilized with sort_array.
SELECT sort_array(collect_set(c)) FROM VALUES
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001') AS t(c);

-- SPARK-56822: collect_list over nanosecond-precision TIMESTAMP_NTZ. The buffer holds the full
-- nanos value, so the sub-microsecond remainder survives and the result element type stays
-- TIMESTAMP_NTZ(9). collect_list order is non-deterministic, so the output is stabilized with
-- sort_array; duplicates are kept and NULLs are dropped.
SELECT sort_array(collect_list(c)) FROM VALUES
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
  (CAST(NULL AS timestamp_ntz(9))) AS t(c);

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
-- DISTINCT over a nanosecond column: exact duplicates are removed, two values sharing epochMicros
-- but differing within the microsecond are both kept, and NULL survives as a single row. Three
-- rows: .000000001, .000000999, NULL.
SELECT DISTINCT c FROM VALUES
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
  (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'),
  (CAST(NULL AS timestamp_ntz(9))) AS t(c)
  ORDER BY c;

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

-- SPARK-57811: a string operand is coerced to the nanosecond timestamp type in comparisons and
-- predicates (not truncated to micros, not promoted to string). The 9th fractional digit is
-- significant, so an off-by-one-nanosecond literal does not compare equal.
SELECT c = '2020-01-02 03:04:05.123456789',
       c = '2020-01-02 03:04:05.123456788',
       c < '2020-01-02 03:04:05.123456790'
  FROM VALUES (TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789') AS t(c);

-- BETWEEN over nanosecond timestamps: only the value inside the sub-microsecond range qualifies.
SELECT c FROM VALUES
  (TIMESTAMP_NTZ '2020-01-02 03:04:05.000000001'),
  (TIMESTAMP_NTZ '2020-01-02 03:04:05.000000009') AS t(c)
  WHERE c BETWEEN '2020-01-02 03:04:05.000000001' AND '2020-01-02 03:04:05.000000005';

-- SPARK-57811: TIMESTAMP_NTZ(p) mirrors micros TimestampNTZType under legacy castDatetimeToString.
-- Micros TimestampNTZType has no arm in findCommonTypeForBinaryComparison, so it stays config-blind
-- and casts the string to the timestamp type even under the legacy flag; nanos NTZ has no arm
-- either and does the same. (Only the LTZ family, like micros TimestampType, promotes the range
-- comparison to string under this flag -- see timestamp-ltz-nanos.sql.) So with both flags set the
-- NTZ range comparison still casts the string to the nanos type, identical to the default config.
SET spark.sql.ansi.enabled=false;
SET spark.sql.legacy.typeCoercion.datetimeToString.enabled=true;
SELECT c = '2020-01-02 03:04:05.123456789',
       c < '2020-01-02 03:04:05.123456790'
  FROM VALUES (TIMESTAMP_NTZ '2020-01-02 03:04:05.123456789') AS t(c);
SET spark.sql.legacy.typeCoercion.datetimeToString.enabled=false;
SET spark.sql.ansi.enabled=true;

-- SPARK-57814: unix_seconds / unix_millis / unix_micros over nanosecond-precision values. The result
-- is a whole BIGINT count of the unit; sub-unit digits (incl. the sub-microsecond remainder) are
-- dropped and NTZ applies no zone shift, so the wall-clock value is read as the epoch instant.
SELECT unix_seconds(TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT unix_millis(TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT unix_micros(TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789');
SELECT unix_micros('2020-01-01 13:24:35.999999999' :: timestamp_ntz(7));
-- Pre-epoch value: floorDiv floors toward -inf, so unix_seconds -> -1 (not 0).
SELECT unix_seconds(TIMESTAMP_NTZ '1969-12-31 23:59:59.500000000');
-- NULL nanosecond timestamp.
SELECT unix_seconds(NULL :: timestamp_ntz(9)), unix_millis(NULL :: timestamp_ntz(9)), unix_micros(NULL :: timestamp_ntz(9));

-- SPARK-57816: date_format / to_char / to_varchar over nanosecond-precision values. The pattern's
-- fractional-second placeholders render up to nanosecond digits; a 9-`S` field is fixed width, so
-- digits below the type's precision floor to zeros rather than being dropped. NTZ renders its
-- wall clock zone-independently. to_char / to_varchar route through the same code path.
SELECT date_format(TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS');
SELECT date_format('2020-01-01 13:24:35.123456789' :: timestamp_ntz(8), 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS');
SELECT date_format('2020-01-01 13:24:35.123456789' :: timestamp_ntz(7), 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS');
SELECT to_char(TIMESTAMP_NTZ '2020-01-01 13:24:35.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS');
SELECT to_char('2020-01-01 13:24:35.123456789' :: timestamp_ntz(7), 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS');
SELECT to_varchar('2020-01-01 13:24:35.123456789' :: timestamp_ntz(8), 'HH:mm:ss.SSSSSSSSS');
-- Pre-epoch value exercises the negative-epoch path.
SELECT date_format(TIMESTAMP_NTZ '1960-01-01 13:24:35.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS');
-- NULL nanosecond timestamp.
SELECT date_format(NULL :: timestamp_ntz(9), 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS');

-- SPARK-57821: date_trunc keeps the nanosecond type/family and zeroes the whole fraction (including
-- the sub-microsecond digits); MICROSECOND keeps epochMicros and only drops nanosWithinMicro. NTZ
-- is zone-independent, so DAY/HOUR read the wall clock and never shift.
SELECT date_trunc('SECOND', TIMESTAMP_NTZ '2020-01-01 12:34:56.123456789');
SELECT date_trunc('MICROSECOND', TIMESTAMP_NTZ '2020-01-01 12:34:56.123456789');
SELECT date_trunc('HOUR', '2020-01-01 12:34:56.123456789' :: timestamp_ntz(9));
SELECT date_trunc('DAY', '2020-06-21 23:30:00.000000123' :: timestamp_ntz(7));
-- An unsupported (sub-microsecond) unit yields NULL; the result still carries the nanos type.
SELECT date_trunc('NANOSECOND', TIMESTAMP_NTZ '2020-01-01 12:34:56.123456789');

-- SPARK-57837: localtimestamp(p) with a nanosecond precision returns TIMESTAMP_NTZ(p). The values
-- are non-deterministic, so only the (deterministic) result type and query-stable self-equality
-- are checked. Precision 6 keeps the standard microsecond TIMESTAMP_NTZ.
SELECT typeof(localtimestamp(9)), typeof(localtimestamp(8)), typeof(localtimestamp(7));
SELECT typeof(localtimestamp()), typeof(localtimestamp(6));
-- A foldable (constant) precision expression is accepted.
SELECT typeof(localtimestamp(8 + 1));
-- All references to localtimestamp(p) within a query see the same value.
SELECT localtimestamp(9) = localtimestamp(9);
-- Out-of-range precision is rejected.
SELECT localtimestamp(3);
SELECT localtimestamp(10);

-- SPARK-57841: end-to-end coverage for operators that ride on the resolved widening (SPARK-57454)
-- and complex-type access over nanosecond values. Every case turns on the SUB-MICROSECOND remainder
-- (.000000001 vs .000000999 share a microsecond; only the full nanos value tells them apart) or on
-- cross-precision widening. Multi-row queries end in a top-level ORDER BY so the golden output order
-- is meaningful (SQLQueryTestSuite re-sorts otherwise). NTZ is zone-independent; the LTZ file mirrors
-- these in the session zone.

-- INTERSECT / EXCEPT distinguish the sub-microsecond remainder. A micro-only set op would wrongly
-- merge .000000001 and .000000999; here INTERSECT keeps only the common value and EXCEPT removes it.
SELECT c FROM (SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001' AS c
    UNION ALL SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999')
  INTERSECT SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001' ORDER BY c;
SELECT c FROM (SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001' AS c
    UNION ALL SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999')
  EXCEPT SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001' ORDER BY c;
-- Mixed-precision set op widens to the wider precision; the equal instant matches after widening.
SELECT typeof(c), c FROM (
    (SELECT '2020-01-01 00:00:00.0000009' :: timestamp_ntz(7) AS c)
     INTERSECT (SELECT '2020-01-01 00:00:00.000000900' :: timestamp_ntz(9))) ORDER BY c;

-- BETWEEN on a sub-microsecond boundary: the bounds share the microsecond with the probe, so only
-- the full nanos value decides inclusivity. .000000500 is inside [.000000001, .000000999];
-- .000001000 (next microsecond) is outside.
SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000500'
    BETWEEN TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'
        AND TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999';
SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000001000'
    BETWEEN TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'
        AND TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999';
-- Mixed-precision BETWEEN widens the bounds to the probe's precision.
SELECT '2020-01-01 00:00:00.000000500' :: timestamp_ntz(9)
    BETWEEN '2020-01-01 00:00:00.0000001' :: timestamp_ntz(7)
        AND TIMESTAMP_NTZ '2020-01-01 00:00:00.000001';

-- if / nvl / ifnull preserve the nanos type and widen mixed-precision branches to the wider type.
SELECT typeof(v), v FROM (SELECT if(true,
    '2020-01-01 00:00:00.0000001' :: timestamp_ntz(7),
    TIMESTAMP_NTZ '2020-01-01 00:00:00.123456789') AS v);
SELECT typeof(v), v FROM (SELECT nvl(
    CAST(NULL AS timestamp_ntz(9)),
    TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') AS v);
SELECT ifnull(TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001', CAST(NULL AS timestamp_ntz(9)));

-- IN (subquery): the semi-join matches on the full nanos key, so only the .000000999 row qualifies.
SELECT k FROM VALUES
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') AS t(k)
  WHERE k IN (SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') ORDER BY k;

-- Scalar subquery in projection returns the nanos value and carries the nanos type;
-- the sub-microsecond precision survives scalar-subquery result boxing.
SELECT (SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999');
SELECT typeof((SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'));
-- A NULL scalar subquery still carries the nanos type.
SELECT typeof((SELECT CAST(NULL AS timestamp_ntz(9))));
-- Scalar subquery in a WHERE comparison: the sub-microsecond value decides the match, so only
-- the .000000999 row qualifies (not the .000000001 row).
SELECT k FROM VALUES
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') AS t(k)
  WHERE k = (SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') ORDER BY k;

-- EXISTS (correlated on a nanos equality): the outer row is kept iff a matching nanos key exists
-- in the subquery relation. Only the .000000999 row correlates to s.v.
SELECT k FROM VALUES
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') AS t(k)
  WHERE EXISTS (SELECT 1 FROM VALUES
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') AS s(v)
    WHERE s.v = t.k) ORDER BY k;

-- NOT EXISTS (correlated on a nanos equality): the opposite; the outer row is kept iff no matching
-- nanos key exists. The subquery holds only .000000001, so the .000000999 row survives.
SELECT k FROM VALUES
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') AS t(k)
  WHERE NOT EXISTS (SELECT 1 FROM VALUES
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001') AS s(v)
    WHERE s.v = t.k) ORDER BY k;

-- NOT IN (subquery): anti-semi-join on the full nanos key. The .000000999 row is in the subquery
-- set (excluded); the .000000001 row is not (kept). Sub-microsecond precision decides membership --
-- the two values differ in the nanosecond digit, not by rounding error.
SELECT k FROM VALUES
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') AS t(k)
  WHERE k NOT IN (SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') ORDER BY k;

-- Mixed-precision NOT IN widens the probe to p=9 before the anti-join. The p=7 value .0000009
-- becomes .000000900 at p=9, which is not .000000999, so the row is not in the set and is kept.
SELECT k FROM VALUES
    ('2020-01-01 00:00:00.0000009' :: timestamp_ntz(7)) AS t(k)
  WHERE k NOT IN (SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') ORDER BY k;

-- NOT IN with a NULL in the subquery set: three-valued logic. For the row that does not equal the
-- non-null member, the comparison against NULL is UNKNOWN, so NOT IN is UNKNOWN and the row is
-- filtered out; the row that equals the non-null member is a definite match and also excluded.
-- The result is therefore empty.
SELECT k FROM VALUES
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001'),
    (TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999') AS t(k)
  WHERE k NOT IN (SELECT TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'
                  UNION ALL SELECT CAST(NULL AS timestamp_ntz(9))) ORDER BY k;

-- explode(array<ts_nanos>) yields one row per element, each keeping the nanos type and value.
SELECT typeof(col), col FROM (SELECT explode(array(
    TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001',
    TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'))) ORDER BY col;

-- element_at over array<ts_nanos> (1-based) returns the addressed element unchanged.
SELECT element_at(array(
    TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001',
    TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'), 2);

-- struct-field extraction reads the nanos value back out of a struct.
SELECT (named_struct('f', TIMESTAMP_NTZ '2020-01-01 00:00:00.123456789')).f;

-- map lookup by string key and by nanosecond key (GetMapValue / element_at over a nanos-keyed map).
-- The nanos-keyed lookup must consult the full sub-microsecond value: looking up .000000999 returns
-- 'b', not 'a', even though both keys share the microsecond.
SELECT map('k', TIMESTAMP_NTZ '2020-01-01 00:00:00.123456789')['k'];
SELECT map(TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001', 'a',
           TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999', 'b')[
       TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999'];
SELECT element_at(map(TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001', 'a',
           TIMESTAMP_NTZ '2020-01-01 00:00:00.000000999', 'b'),
       TIMESTAMP_NTZ '2020-01-01 00:00:00.000000001');
