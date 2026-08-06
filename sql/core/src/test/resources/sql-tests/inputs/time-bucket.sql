-- time_bucket function tests

-- Pin session timezone to UTC. With UTC as the session zone, TIMESTAMP (LTZ)
-- bucketing produces the same results as TIMESTAMP_NTZ. The session-zone-aware
-- behavior is exercised in a dedicated `SET TIME ZONE 'America/Los_Angeles'`
-- section at the end of this file.
SET TIME ZONE 'UTC';


-- Error: bucket_size must be positive

-- Zero literal (DT, YM MONTH, YM YEAR)
SELECT time_bucket(INTERVAL '0' SECOND, TIMESTAMP '2024-01-01 11:00:00');
SELECT time_bucket(INTERVAL '0' MONTH, TIMESTAMP '2024-01-01 11:00:00');
SELECT time_bucket(INTERVAL '0' YEAR, TIMESTAMP '2024-01-01 11:00:00');

-- Negative literal (DT, YM MONTH, YM YEAR)
SELECT time_bucket(INTERVAL '-15' MINUTE, TIMESTAMP '2024-01-01 11:00:00');
SELECT time_bucket(INTERVAL '-1' MONTH, TIMESTAMP '2024-01-01 11:00:00');
SELECT time_bucket(INTERVAL '-1' YEAR, TIMESTAMP '2024-01-01 11:00:00');

-- Foldable arithmetic producing zero or negative
SELECT time_bucket(INTERVAL '15' MINUTE - INTERVAL '15' MINUTE, TIMESTAMP '2024-06-20 09:47:00');
SELECT time_bucket(INTERVAL '5' MINUTE - INTERVAL '15' MINUTE, TIMESTAMP '2024-06-20 09:47:00');
SELECT time_bucket(INTERVAL '3' MONTH - INTERVAL '3' MONTH, TIMESTAMP '2024-06-20 09:47:00');
SELECT time_bucket(INTERVAL '1' MONTH - INTERVAL '3' MONTH, TIMESTAMP '2024-06-20 09:47:00');


-- Error: argument types

-- ts and origin must be the same TIMESTAMP flavor (both TIMESTAMP or both TIMESTAMP_NTZ)
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-01-01 11:27:00', TIMESTAMP_NTZ '2024-01-01 00:00:00');
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP_NTZ '2024-01-01 11:27:00', TIMESTAMP '2024-01-01 00:00:00');

-- bucket_size must be an interval (not a string)
SELECT time_bucket('15 minutes', TIMESTAMP '2024-01-15 10:23:00');

-- ts must be TIMESTAMP or TIMESTAMP_NTZ (not DATE or string)
SELECT time_bucket(INTERVAL '15' MINUTE, DATE '2024-01-15');
SELECT time_bucket(INTERVAL '15' MINUTE, '2024-01-15 10:23:00');

-- origin must be TIMESTAMP or TIMESTAMP_NTZ (not DATE or string)
SELECT time_bucket(INTERVAL '15' MINUTE, TIMESTAMP '2024-01-15 10:23:00', DATE '2024-01-01');
SELECT time_bucket(INTERVAL '15' MINUTE, TIMESTAMP '2024-01-15 10:23:00', '2024-01-01 00:00:00');


-- Error: bucket_size and origin must be foldable

-- Non-foldable bucket_size via column reference (DT and YM)
SELECT time_bucket(bs, TIMESTAMP '2024-06-20 09:47:00') FROM VALUES (INTERVAL '1' HOUR) tab(bs);
SELECT time_bucket(bs, TIMESTAMP '2024-06-20 09:47:00') FROM VALUES (INTERVAL '1' MONTH) tab(bs);

-- Non-foldable bucket_size via scalar subquery (DT and YM)
SELECT time_bucket((SELECT INTERVAL '1' HOUR), TIMESTAMP '2024-06-20 09:47:00');
SELECT time_bucket((SELECT INTERVAL '1' MONTH), TIMESTAMP '2024-06-20 09:47:00');

-- Non-foldable origin via column reference (DT and YM bucket)
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-06-20 09:47:00', o) FROM VALUES (TIMESTAMP '2024-01-01 00:00:00') tab(o);
SELECT time_bucket(INTERVAL '1' MONTH, TIMESTAMP '2024-06-20 09:47:00', o) FROM VALUES (TIMESTAMP '2024-01-01 00:00:00') tab(o);

-- Non-foldable origin via scalar subquery
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-06-20 09:47:00', (SELECT TIMESTAMP '2024-01-01 00:00:00'));


-- Error: wrong number of arguments

-- 1-arg (too few)
SELECT time_bucket(INTERVAL '1' HOUR);

-- 4-arg (too many)
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-01-01 11:27:00', TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '1970-01-01 00:00:00');


-- NULL propagation

-- Typed NULL for bucket_size, ts, or origin
SELECT time_bucket(NULL, TIMESTAMP '2024-01-01 11:27:00');
SELECT time_bucket(INTERVAL '1' HOUR, NULL);
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-01-01 11:27:00', NULL);

-- Both ts and origin NULL
SELECT time_bucket(INTERVAL '1' HOUR, NULL, NULL);

-- NULL ts with explicit typed origin drives ts retyping via the builder
SELECT time_bucket(INTERVAL '1' HOUR, NULL, TIMESTAMP '2024-01-01 00:00:00');
SELECT time_bucket(INTERVAL '1' HOUR, NULL, TIMESTAMP_NTZ '2024-01-01 00:00:00');


-- DayTimeInterval buckets: default (epoch) origin

-- 15-minute bucket
SELECT time_bucket(INTERVAL '15' MINUTE, TIMESTAMP '2024-01-01 11:27:00');

-- 1-hour bucket
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-01-01 11:27:00');

-- 7-day (weekly) bucket (epoch is Thursday, so buckets run Thu-Wed)
SELECT time_bucket(INTERVAL '7' DAY, TIMESTAMP '2024-01-10 11:27:00');

-- Compound DayTimeInterval (1 day 30 minutes)
SELECT time_bucket(INTERVAL '1 00:30' DAY TO MINUTE, TIMESTAMP '2024-06-20 10:00:00');

-- 1-microsecond bucket (finest supported precision)
SELECT time_bucket(INTERVAL '0.000001' SECOND, TIMESTAMP '2024-06-20 10:00:00.123456');

-- DayTimeInterval bucket on TIMESTAMP_NTZ
SELECT time_bucket(INTERVAL '15' MINUTE, TIMESTAMP_NTZ '2024-01-01 11:27:00');


-- DayTimeInterval buckets: explicit origin

-- Custom origin at :05 shifts the grid so ts 11:27 lands in [11:05, 12:05)
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-01-01 11:27:00', TIMESTAMP '1970-01-01 00:05:00');

-- ts exactly on a bucket boundary returns ts
SELECT time_bucket(INTERVAL '15' MINUTE, TIMESTAMP '2024-01-01 11:15:00');

-- ts exactly equal to origin returns origin
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-01-01 11:27:00', TIMESTAMP '2024-01-01 11:27:00');

-- DT origin after ts
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-01-01 11:27:00', TIMESTAMP '2025-01-01 00:30:00');

-- DayTimeInterval 3-arg with TIMESTAMP_NTZ
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP_NTZ '2024-01-15 10:23:00', TIMESTAMP_NTZ '2024-01-15 00:30:00');


-- YearMonthInterval buckets: default (epoch) origin

-- 1-month bucket
SELECT time_bucket(INTERVAL '1' MONTH, TIMESTAMP '2024-03-15 11:27:00');

-- 3-month (quarterly) bucket
SELECT time_bucket(INTERVAL '3' MONTH, TIMESTAMP '2024-05-15 10:00:00');

-- 1-year bucket
SELECT time_bucket(INTERVAL '1' YEAR, TIMESTAMP '2024-05-15 10:00:00');

-- Compound YearMonthInterval (1 year 3 months = 15 months)
SELECT time_bucket(INTERVAL '1-3' YEAR TO MONTH, TIMESTAMP '2024-06-20 10:00:00');

-- YearMonthInterval bucket on TIMESTAMP_NTZ
SELECT time_bucket(INTERVAL '1' MONTH, TIMESTAMP_NTZ '2024-03-15 11:27:00');

-- ts exactly on a bucket boundary
SELECT time_bucket(INTERVAL '1' MONTH, TIMESTAMP '2024-03-01 00:00:00');


-- YearMonthInterval buckets: explicit origin

-- Origin on 15th aligns grid to the 15th of each month
SELECT time_bucket(INTERVAL '1' MONTH, TIMESTAMP '2024-03-20 09:00:00', TIMESTAMP '1970-01-15 00:00:00');

-- ts exactly equal to origin returns origin
SELECT time_bucket(INTERVAL '1' MONTH, TIMESTAMP '2024-03-15 10:00:00', TIMESTAMP '2024-03-15 10:00:00');

-- End-of-month capping + step-back: origin Jan 31, 1-month bucket -> 2024-02-29 (leap year)
SELECT time_bucket(INTERVAL '1' MONTH, TIMESTAMP '2024-03-01 12:00:00', TIMESTAMP '1970-01-31 00:00:00');

-- Leap-year capping: origin Feb 29, 1-year bucket -> 2025-02-28 (non-leap target)
SELECT time_bucket(INTERVAL '1' YEAR, TIMESTAMP '2025-03-01 00:00:00', TIMESTAMP '2024-02-29 00:00:00');

-- YM origin after ts
SELECT time_bucket(INTERVAL '3' MONTH, TIMESTAMP '2024-02-15 10:00:00', TIMESTAMP '2024-08-01 00:00:00');

-- YearMonthInterval 3-arg with TIMESTAMP_NTZ and custom origin
SELECT time_bucket(INTERVAL '3' MONTH, TIMESTAMP_NTZ '2024-08-20 14:30:00', TIMESTAMP_NTZ '2024-01-01 00:00:00');


-- Pre-epoch timestamps and origins

-- Pre-epoch ts, 1-day bucket
SELECT time_bucket(INTERVAL '1' DAY, TIMESTAMP '1969-12-31 23:30:00');

-- Pre-epoch ts, 1-hour bucket
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '1969-12-31 23:30:00');

-- Pre-epoch origin with post-epoch ts
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-01-01 11:27:00', TIMESTAMP '1960-06-15 00:30:00');

-- Pre-epoch ts, YearMonthInterval bucket
SELECT time_bucket(INTERVAL '1' MONTH, TIMESTAMP '1968-07-15 10:00:00');


-- Foldable expressions (bucket_size, origin, and ts are folded at analysis time)

-- Foldable DayTimeInterval arithmetic in bucket_size
SELECT time_bucket(INTERVAL '10' MINUTE + INTERVAL '5' MINUTE, TIMESTAMP '2024-06-20 09:47:00');
SELECT time_bucket(INTERVAL '1' HOUR - INTERVAL '15' MINUTE, TIMESTAMP '2024-06-20 09:47:00');

-- Foldable YearMonthInterval arithmetic in bucket_size
SELECT time_bucket(INTERVAL '2' MONTH + INTERVAL '1' MONTH, TIMESTAMP '2024-06-20 09:47:00');

-- Foldable arithmetic in origin
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-06-20 09:47:00', TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '5' MINUTE);

-- Foldable arithmetic in ts
SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2024-06-20 09:47:00' + INTERVAL '30' MINUTE);


-- Column reference as ts

-- DayTimeInterval bucket over TIMESTAMP column (with one NULL row)
SELECT t, time_bucket(INTERVAL '1' HOUR, t) AS bucket
  FROM VALUES (TIMESTAMP '2024-01-15 10:23:00'), (TIMESTAMP '2024-01-15 14:45:00'), (CAST(NULL AS TIMESTAMP)) tab(t)
  ORDER BY t;

-- DayTimeInterval bucket over TIMESTAMP_NTZ column
SELECT t, time_bucket(INTERVAL '15' MINUTE, t) AS bucket
  FROM VALUES (TIMESTAMP_NTZ '2024-01-15 10:23:00'), (TIMESTAMP_NTZ '2024-01-15 14:07:00') tab(t)
  ORDER BY t;

-- YearMonthInterval bucket over TIMESTAMP column
SELECT t, time_bucket(INTERVAL '1' MONTH, t) AS bucket
  FROM VALUES (TIMESTAMP '2024-03-15 10:23:00'), (TIMESTAMP '2024-06-01 00:00:00') tab(t)
  ORDER BY t;


-- Bucketing in a non-UTC session: TIMESTAMP (LTZ) values bucket in the session time
-- zone, so monthly and daily boundaries land at local calendar boundaries across DST
-- (America/Los_Angeles springs forward 2024-03-10 and falls back 2024-11-03). NTZ
-- values bucket in UTC. This section is at the end of the file so no later test
-- accidentally inherits the non-UTC zone.
SET TIME ZONE 'America/Los_Angeles';
SELECT t, time_bucket(INTERVAL '1' MONTH, t) AS bucket
  FROM VALUES
    (TIMESTAMP '2024-02-15 10:00:00'),
    (TIMESTAMP '2024-03-15 10:00:00'),
    (TIMESTAMP '2024-04-15 10:00:00') tab(t)
  ORDER BY t;
SELECT t, time_bucket(INTERVAL '1' MONTH, CAST(t AS TIMESTAMP_NTZ)) AS bucket
  FROM VALUES
    (TIMESTAMP '2024-02-15 10:00:00'),
    (TIMESTAMP '2024-03-15 10:00:00'),
    (TIMESTAMP '2024-04-15 10:00:00') tab(t)
  ORDER BY t;
-- Daily bucket on LTZ across the spring-forward day: each ts buckets to local midnight.
SELECT t, time_bucket(INTERVAL '1' DAY, t) AS bucket
  FROM VALUES
    (TIMESTAMP '2024-03-09 12:00:00'),
    (TIMESTAMP '2024-03-10 12:00:00'),
    (TIMESTAMP '2024-03-11 12:00:00') tab(t)
  ORDER BY t;
-- Daily bucket on LTZ across the fall-back day (2024-11-03): each ts buckets to local
-- midnight of its own day even though Nov 3 spans 25 UTC hours.
SELECT t, time_bucket(INTERVAL '1' DAY, t) AS bucket
  FROM VALUES
    (TIMESTAMP '2024-11-02 12:00:00'),
    (TIMESTAMP '2024-11-03 12:00:00'),
    (TIMESTAMP '2024-11-04 12:00:00') tab(t)
  ORDER BY t;
-- Compound day-time bucket (36h) on LTZ across the fall-back day.
SELECT t, time_bucket(INTERVAL '36' HOUR, t, TIMESTAMP '2024-11-01 00:00:00') AS bucket
  FROM VALUES
    (TIMESTAMP '2024-11-05 11:30:00') tab(t);
