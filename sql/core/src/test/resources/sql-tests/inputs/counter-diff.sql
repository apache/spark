-- Tests for the counter_diff window function.

SET TIME ZONE 'UTC';

------------------------------------------------------------
-- Basic semantics
------------------------------------------------------------

-- Monotonically increasing counter: each diff is current - previous.
SELECT t, c, counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, 100), (2, 200), (3, 400) AS tab(t, c)
ORDER BY t;

-- Single-row input: the only row has no predecessor and returns NULL.
SELECT t, counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, 50) AS tab(t, c)
ORDER BY t;

-- Counter reset detected by value decrease: the row after the drop returns NULL.
SELECT t, c, counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, 100), (2, 200), (3, 400), (4, 50), (5, 100) AS tab(t, c)
ORDER BY t;

-- Equal counter values produce a diff of 0.
SELECT t, c, counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, 100), (2, 100), (3, 200) AS tab(t, c)
ORDER BY t;

-- NULL counter rows have NULL as the result and are skipped when calculating differences.
SELECT t, c, counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, 100), (2, CAST(NULL AS INT)), (3, 200) AS tab(t, c)
ORDER BY t;

-- Each partition has its own first row and prior values.
SELECT m, t, c, counter_diff(c) OVER (PARTITION BY m ORDER BY t) AS diff
FROM VALUES
  ('a', 1, 100), ('a', 2, 200),
  ('b', 1, 10),  ('b', 2, 30)
AS tab(m, t, c)
ORDER BY m, t;

------------------------------------------------------------
-- Numeric types
------------------------------------------------------------

-- counter_diff supports all numeric types:
-- DOUBLE, FLOAT, BIGINT, LONG, INT, SMALLINT, TINYINT, DECIMAL.
SELECT t,
  counter_diff(d)   OVER (ORDER BY t) AS d_diff,
  counter_diff(f)   OVER (ORDER BY t) AS f_diff,
  counter_diff(b)   OVER (ORDER BY t) AS b_diff,
  counter_diff(l)   OVER (ORDER BY t) AS l_diff,
  counter_diff(i)   OVER (ORDER BY t) AS i_diff,
  counter_diff(si)  OVER (ORDER BY t) AS si_diff,
  counter_diff(ti)  OVER (ORDER BY t) AS ti_diff,
  counter_diff(dec) OVER (ORDER BY t) AS dec_diff
FROM VALUES
  (1, 1.5D, CAST(1.5 AS FLOAT), CAST(100 AS BIGINT), CAST(100 AS LONG),
     CAST(100 AS INT), CAST(100 AS SMALLINT), CAST(10 AS TINYINT),
     CAST(10.5 AS DECIMAL(10,2))),
  (2, 3.5D, CAST(3.5 AS FLOAT), CAST(300 AS BIGINT), CAST(300 AS LONG),
     CAST(300 AS INT), CAST(300 AS SMALLINT), CAST(30 AS TINYINT),
     CAST(20.5 AS DECIMAL(10,2)))
AS tab(t, d, f, b, l, i, si, ti, dec)
ORDER BY t;

------------------------------------------------------------
-- High-precision DECIMAL inputs
------------------------------------------------------------
-- Decimal subtraction normally widens the result type to handle possible overflow.
-- For counter_diff, since counters cannot be negative, there is no risk of overflow, and no
-- need to widen the result type, so we subtract directly in the input type.
-- These tests verify that the result type is not widened, and that no precision is lost for
-- large precision and scale.

-- DECIMAL(38, 38): normal subtraction would be of type DECIMAL(38, 37).
SELECT t, counter_diff(c) OVER (ORDER BY t) AS diff, c - lag(c) OVER (ORDER BY t) AS diff_subtract
FROM VALUES
  (1, CAST(0.10000000000000000000000000000000000001 AS DECIMAL(38, 38))),
  (2, CAST(0.10000000000000000000000000000000000002 AS DECIMAL(38, 38)))
AS tab(t, c) ORDER BY t;

-- DECIMAL(38, 6): normal subtraction would be of type DECIMAL(38, 6).
SELECT t, counter_diff(c) OVER (ORDER BY t) AS diff, c - lag(c) OVER (ORDER BY t) AS diff_subtract
FROM VALUES
  (1, CAST(12345678901234567890123456789012.123456 AS DECIMAL(38, 6))),
  (2, CAST(12345678901234567890123456789012.123457 AS DECIMAL(38, 6)))
AS tab(t, c) ORDER BY t;

-- DECIMAL(10, 2): normal subtraction would be of type DECIMAL(11, 2).
SELECT t, counter_diff(c) OVER (ORDER BY t) AS diff, c - lag(c) OVER (ORDER BY t) AS diff_subtract
FROM VALUES
  (1, CAST(99999999.98 AS DECIMAL(10, 2))),
  (2, CAST(99999999.99 AS DECIMAL(10, 2)))
AS tab(t, c) ORDER BY t;

------------------------------------------------------------
-- NULL type inputs
------------------------------------------------------------

-- Untyped NULL counters are treated as DOUBLE.
SELECT t, counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, NULL), (2, NULL), (3, NULL) AS tab(t, c)
ORDER BY t;

-- Untyped NULL start_time is treated as TIMESTAMP.
-- The counter behavior is unaffected because NULL start_time skips the reset check.
SELECT t, counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES (1, 100, NULL), (2, 200, NULL) AS tab(t, c, st)
ORDER BY t;

-- Explicitly-typed all-NULL INT counter: type stays INT.
SELECT t, counter_diff(c) OVER (ORDER BY t) AS diff
FROM (SELECT t, CAST(c AS INT) AS c
        FROM VALUES (1, NULL), (2, NULL), (3, NULL) AS tab(t, c))
ORDER BY t;

------------------------------------------------------------
-- Negative counter values are runtime errors
------------------------------------------------------------

-- INT.
SELECT counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, -5) AS tab(t, c);

-- DOUBLE.
SELECT counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, -5.0D) AS tab(t, c);

-- DECIMAL: the error message preserves the configured scale.
SELECT counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, CAST(-5.5 AS DECIMAL(10, 3))) AS tab(t, c);

-- -Infinity is treated as a negative value.
SELECT counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, DOUBLE('-Infinity')) AS tab(t, c);

-- Negative value after a NULL row still results in an error.
SELECT counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, 100), (2, CAST(NULL AS INT)), (3, -5) AS tab(t, c);

------------------------------------------------------------
-- Special floating-point values
------------------------------------------------------------

-- Positive Infinity participates in arithmetic:
-- +Infinity - 100 = +Infinity.
-- 200 - +Infinity => Reset.
-- +Infinity - 200 = +Infinity.
-- +Infinity - +Infinity = NaN.
SELECT t, counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES
  (1, 100.0D), (2, DOUBLE('Infinity')), (3, 200.0D),
  (4, DOUBLE('Infinity')), (5, DOUBLE('Infinity')) AS tab(t, c)
ORDER BY t;

-- NaN values are greater than all other numeric values, so:
-- any value -> NaN => NaN
-- NaN -> any non-NaN value => Reset
SELECT t, counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES
  (1, 100.0D), (2, DOUBLE('NaN')), (3, DOUBLE('NaN')), (4, 200.0D),
  (5, 400.0D), (6, DOUBLE('NaN')), (7, 50.0D), (8, 100.0D) AS tab(t, c)
ORDER BY t;

------------------------------------------------------------
-- Constants and foldable expressions as arguments
------------------------------------------------------------

-- Constant counter: every diff except the first is 0.
SELECT t, counter_diff(1) OVER (ORDER BY t) AS diff
FROM RANGE(1, 5) AS tab(t)
ORDER BY t;

-- Foldable expression counter (1 + 1) behaves like the constant case.
SELECT t, counter_diff(1 + 1) OVER (ORDER BY t) AS diff
FROM RANGE(1, 5) AS tab(t)
ORDER BY t;

-- Constant start_time alongside constant counter.
SELECT t, counter_diff(1, TIMESTAMP '2026-01-01 00:00:00')
  OVER (ORDER BY t) AS diff
FROM RANGE(1, 5) AS tab(t)
ORDER BY t;

-- Foldable counter and foldable start_time.
SELECT t, counter_diff(
    1 + 1,
    TIMESTAMP '2026-01-01 00:00:00' + INTERVAL '1' SECOND
  ) OVER (ORDER BY t) AS diff
FROM RANGE(1, 5) AS tab(t)
ORDER BY t;

------------------------------------------------------------
-- Combined with other window functions
------------------------------------------------------------

-- Compare counter_diff against lag and lag IGNORE NULLS over the same ordering.
SELECT t, c,
       counter_diff(c) OVER (ORDER BY t) AS diff,
       c - lag(c) OVER (ORDER BY t) AS d1,
       c - lag(c) IGNORE NULLS OVER (ORDER BY t) AS d2
FROM VALUES (1, 100), (2, CAST(NULL AS INT)), (3, 300), (4, 150) AS tab(t, c)
ORDER BY t;

-- Mix counter_diff with avg, lead, max over different frames.
SELECT t, c,
       counter_diff(c) OVER (ORDER BY t) AS diff,
       avg(c) OVER (ORDER BY t ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS local_avg,
       lead(c) OVER (ORDER BY t) AS nc,
       max(c) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS some_max
FROM VALUES (1, 100), (2, 200), (3, 150), (4, 400), (5, 500), (6, 600) AS tab(t, c)
ORDER BY t;

-- Multiple windows with different partitions in the same SELECT.
SELECT m, t, c,
       counter_diff(c) OVER (PARTITION BY m ORDER BY t) AS diff,
       avg(c) OVER (PARTITION BY m ORDER BY t
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS local_avg,
       max(c) OVER (PARTITION BY t) AS max_c
FROM VALUES
  ('a', 1, 100), ('a', 2, 200), ('a', 3, 150), ('a', 4, 300),
  ('b', 1, 10),  ('b', 2, 30),  ('b', 3, 60),  ('b', 4, 100)
AS tab(m, t, c)
ORDER BY m, t;

------------------------------------------------------------
-- start_time parameter
------------------------------------------------------------

-- start_time advance triggers a reset even when the counter increases.
SELECT t, c,
  counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES
  (1, TIMESTAMP '2026-01-01 00:00:00', 100),
  (2, TIMESTAMP '2026-01-01 00:00:00', 200),
  (3, TIMESTAMP '2026-01-01 00:02:31', 400)
AS tab(t, st, c)
ORDER BY t;

-- Equal start_time across rows: behavior matches the case with no start time.
SELECT t, c,
  counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES
  (1, TIMESTAMP '2026-01-01 00:00:00', 100),
  (2, TIMESTAMP '2026-01-01 00:00:00', 200)
AS tab(t, st, c)
ORDER BY t;

-- NULL start_time skips the start time reset check on that row and the next row.
SELECT t, c,
  counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES
  (1, TIMESTAMP '2026-01-01 00:00:00', 100),
  (2, CAST(NULL AS TIMESTAMP),         200),
  (3, TIMESTAMP '2026-01-01 00:01:00', 300)
AS tab(t, st, c)
ORDER BY t;

-- Decreasing start_time is a runtime error.
SELECT counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES
  (1, TIMESTAMP '2026-01-01 00:05:00', 100),
  (2, TIMESTAMP '2026-01-01 00:01:00', 200)
AS tab(t, st, c);

-- Negative counter still raises in the start_time form.
SELECT counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES
  (1, TIMESTAMP '2026-01-01 00:00:00', -1)
AS tab(t, st, c);

-- TIMESTAMP_NTZ start_time is accepted; same reset behavior as TIMESTAMP.
SELECT t, c,
  counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES
  (1, TIMESTAMP_NTZ '2026-01-01 00:00:00', 100),
  (2, TIMESTAMP_NTZ '2026-01-01 00:00:00', 200),
  (3, TIMESTAMP_NTZ '2026-01-01 00:05:00', 300)
AS tab(t, st, c)
ORDER BY t;

-- Partitioned start_time: each partition tracks its own previous start_time.
SELECT m, t, c, counter_diff(c, st) OVER (PARTITION BY m ORDER BY t) AS diff
FROM VALUES
  ('a', 1, TIMESTAMP '2026-01-01 00:00:00', 100),
  ('a', 2, TIMESTAMP '2026-01-01 00:00:00', 200),
  ('a', 3, TIMESTAMP '2026-01-01 00:05:00', 500),
  ('b', 1, TIMESTAMP '2026-01-01 00:00:00', 10),
  ('b', 2, TIMESTAMP '2026-01-01 00:00:00', 20)
AS tab(m, t, st, c)
ORDER BY m, t;

-- A NULL-counter row is skipped before the start_time check, so a same-row
-- start_time decrease is absorbed when the counter is NULL.
SELECT t, counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES
  (1, TIMESTAMP '2026-01-01 00:05:00', 100),
  (2, TIMESTAMP '2026-01-01 00:01:00', CAST(NULL AS INT))
AS tab(t, st, c)
ORDER BY t;

-- ...but the next non-NULL row still observes the decrease and raises.
SELECT counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES
  (1, TIMESTAMP '2026-01-01 00:05:00', 100),
  (2, TIMESTAMP '2026-01-01 00:01:00', CAST(NULL AS INT)),
  (3, TIMESTAMP '2026-01-01 00:01:00', 300)
AS tab(t, st, c);

-- A NULL-counter row is skipped before the start_time check, so a same-row
-- start_time increase is absorbed when the counter is NULL, but is resurfaced
-- on the next non-NULL row.
SELECT t, counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES
  (1, TIMESTAMP '2026-01-01 00:05:00', 100),
  (2, TIMESTAMP '2026-01-01 00:06:00', CAST(NULL AS INT)),
  (3, TIMESTAMP '2026-01-01 00:07:00', 300)
AS tab(t, st, c)
ORDER BY t;

------------------------------------------------------------
-- End-to-end: bucket per-row diffs by hour using a CTE
------------------------------------------------------------

-- 8 measurements every 30 min; start_time changes at id=4 -> single reset.
-- The hourly SUM of diffs yields the per-hour total counter increase.
WITH gen AS (
  SELECT
    TIMESTAMPADD(MINUTE, id * 30, TIMESTAMP '2026-01-01 00:00:00') AS ts,
    CASE WHEN id < 4 THEN TIMESTAMP '2026-01-01 00:00:00'
         ELSE TIMESTAMP '2026-01-01 02:00:00' END AS st,
    CASE WHEN id < 4 THEN id * 1000 ELSE (id - 4) * 800 END AS c
  FROM RANGE(8) AS r(id)
),
diffs AS (
  SELECT ts, c, counter_diff(c, st) OVER (ORDER BY ts) AS diff
  FROM gen
)
SELECT date_trunc('hour', ts) AS hour_bucket,
       SUM(diff) AS total_diff
FROM diffs GROUP BY 1 ORDER BY 1;

------------------------------------------------------------
-- Frame, arity, and type validation
------------------------------------------------------------

-- counter_diff requires an OVER clause (it is a window function).
SELECT counter_diff(1) AS diff;

-- Window must specify ORDER BY (otherwise the frame is unordered).
SELECT counter_diff(c) OVER () AS diff
FROM VALUES (1) AS tab(c);

-- Counter argument must be NUMERIC: STRING is rejected.
SELECT counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, 'abc') AS tab(t, c);

-- start_time argument must be TIMESTAMP or TIMESTAMP_NTZ: STRING is rejected.
SELECT counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES (1, 'abc', 100) AS tab(t, st, c);

-- A user-supplied frame other than ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW is rejected.
SELECT counter_diff(c) OVER (ORDER BY t ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS diff
FROM VALUES (1, 100), (2, 200) AS tab(t, c);

-- Explicitly specifying the required frame is allowed.
SELECT t, counter_diff(c) OVER (
  ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS diff
FROM VALUES (1, 100), (2, 200) AS tab(t, c)
ORDER BY t;

-- RANGE frames are rejected (counter_diff is row-based).
SELECT counter_diff(c) OVER (
  ORDER BY t RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS diff
FROM VALUES (1, 100), (2, 200) AS tab(t, c);

-- Zero arguments: not accepted.
SELECT counter_diff() OVER (ORDER BY t) AS diff FROM VALUES (1) AS tab(t);

-- More than 2 arguments: not accepted.
SELECT counter_diff(1, TIMESTAMP '2026-01-01', 99) OVER (ORDER BY t) AS diff
FROM VALUES (1) AS tab(t);

-- BOOLEAN counter is rejected.
SELECT counter_diff(c) OVER (ORDER BY t) AS diff
FROM VALUES (1, true), (2, false) AS tab(t, c);

-- DATE start_time is rejected.
SELECT counter_diff(c, st) OVER (ORDER BY t) AS diff
FROM VALUES (1, DATE '2026-01-01', 100) AS tab(t, st, c);
