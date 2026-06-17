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
