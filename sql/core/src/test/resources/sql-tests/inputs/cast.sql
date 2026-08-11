-- cast string representing a valid fractional number to integral should truncate the number
SELECT CAST('1.23' AS int);
SELECT CAST('1.23' AS long);
SELECT CAST('-4.56' AS int);
SELECT CAST('-4.56' AS long);

-- cast string which are not numbers to numeric types
SELECT CAST('abc' AS int);
SELECT CAST('abc' AS long);
SELECT CAST('abc' AS float);
SELECT CAST('abc' AS double);

-- cast string representing a very large number to integral should return null
SELECT CAST('1234567890123' AS int);
SELECT CAST('12345678901234567890123' AS long);

-- cast empty string to integral should return null
SELECT CAST('' AS int);
SELECT CAST('' AS long);
SELECT CAST('' AS float);
SELECT CAST('' AS double);

-- cast null to integral should return null
SELECT CAST(NULL AS int);
SELECT CAST(NULL AS long);

-- cast invalid decimal string to numeric types
SELECT CAST('123.a' AS int);
SELECT CAST('123.a' AS long);
SELECT CAST('123.a' AS float);
SELECT CAST('123.a' AS double);

-- '-2147483648' is the smallest int value
SELECT CAST('-2147483648' AS int);
SELECT CAST('-2147483649' AS int);

-- '2147483647' is the largest int value
SELECT CAST('2147483647' AS int);
SELECT CAST('2147483648' AS int);

-- '-9223372036854775808' is the smallest long value
SELECT CAST('-9223372036854775808' AS long);
SELECT CAST('-9223372036854775809' AS long);

-- '9223372036854775807' is the largest long value
SELECT CAST('9223372036854775807' AS long);
SELECT CAST('9223372036854775808' AS long);

-- cast string to its binary representation
SELECT HEX(CAST('abc' AS binary));

-- cast integral values to their corresponding binary representation
SELECT HEX(CAST(CAST(123 AS byte) AS binary));
SELECT HEX(CAST(CAST(-123 AS byte) AS binary));
SELECT HEX(CAST(123S AS binary));
SELECT HEX(CAST(-123S AS binary));
SELECT HEX(CAST(123 AS binary));
SELECT HEX(CAST(-123 AS binary));
SELECT HEX(CAST(123L AS binary));
SELECT HEX(CAST(-123L AS binary));

DESC FUNCTION boolean;
DESC FUNCTION EXTENDED boolean;
-- TODO: migrate all cast tests here.

-- cast string to interval and interval to string
SELECT CAST('interval 3 month 1 hour' AS interval);
SELECT CAST("interval '3-1' year to month" AS interval year to month);
SELECT CAST("interval '3 00:00:01' day to second" AS interval day to second);
SELECT CAST(interval 3 month 1 hour AS string);
SELECT CAST(interval 3 year 1 month AS string);
SELECT CAST(interval 3 day 1 second AS string);

-- trim string before cast to numeric
select cast(' 1' as tinyint);
select cast(' 1\t' as tinyint);
select cast(' 1' as smallint);
select cast(' 1' as INT);
select cast(' 1' as bigint);
select cast(' 1' as float);
select cast(' 1 ' as DOUBLE);
select cast('1.0 ' as DEC);
select cast('1中文' as tinyint);
select cast('1中文' as smallint);
select cast('1中文' as INT);
select cast('中文1' as bigint);
select cast('1中文' as bigint);

-- trim string before cast to boolean
select cast('\t\t true \n\r ' as boolean);
select cast('\t\n false \t\r' as boolean);
select cast('\t\n xyz \t\r' as boolean);

select cast('23.45' as decimal(4, 2));
select cast('123.45' as decimal(4, 2));
select cast('xyz' as decimal(4, 2));

select cast('2022-01-01' as date);
select cast('a' as date);
select cast('2022-01-01 00:00:00' as timestamp);
select cast('a' as timestamp);
select cast('2022-01-01 00:00:00' as timestamp_ntz);
select cast('a' as timestamp_ntz);

-- SPARK-57211: cast string to nanosecond-precision timestamps TIMESTAMP_NTZ(p)/TIMESTAMP_LTZ(p).
-- Positive cases assert the result type via typeof. Negative cases exercise the ANSI parse-error
-- path and use IS NULL so the result column stays non-nanos (a bare nanos result column is not yet
-- serializable by JDBC/thrift).
select typeof(cast('2022-01-01 00:00:00.123456789' as timestamp_ntz(9)));
select typeof(cast('2022-01-01 00:00:00.123456789' as timestamp_ltz(7)));
select cast('a' as timestamp_ntz(9)) is null;
select cast('a' as timestamp_ltz(9)) is null;

-- SPARK-57256: cast nanosecond-precision timestamps to string. The result column is STRING (the
-- nanos type is only intermediate), so the value is produced by the Cast-to-string expression
-- (ToStringBase). The nanos preview flag defaults to enabled under tests, and LTZ wall-clock inputs
-- round-trip in any session time zone, so these cases stay zone-independent.
-- TIMESTAMP_NTZ(p): precision-driven fraction width and trailing-zero trimming.
select cast(cast('2020-01-01 00:00:00.123456789' as timestamp_ntz(9)) as string);
select cast(cast('2020-01-01 00:00:00.123456789' as timestamp_ntz(8)) as string);
select cast(cast('2020-01-01 00:00:00.123456789' as timestamp_ntz(7)) as string);
select cast(cast('2020-01-01 00:00:00.000000000' as timestamp_ntz(9)) as string);
select cast(cast('2020-01-01 00:00:00.000000999' as timestamp_ntz(9)) as string);
select cast(cast('2020-01-01 00:00:00.000000001' as timestamp_ntz(8)) as string);
-- TIMESTAMP_LTZ(p): exercises the zone-aware path; a wall-clock input round-trips.
select cast(cast('2020-01-01 00:00:00.123456789' as timestamp_ltz(9)) as string);
select cast(cast('2020-01-01 00:00:00.123456789' as timestamp_ltz(7)) as string);
-- Pre-1970 (negative-epoch) values.
select cast(cast('1960-01-01 00:00:00.000000001' as timestamp_ntz(9)) as string);
select cast(cast('1960-01-01 00:00:00.123456789' as timestamp_ltz(7)) as string);
-- Complex types cast to string (recursive element path, including a nested NULL).
select cast(array(cast('2020-01-01 00:00:00.123456789' as timestamp_ntz(9)), cast(null as timestamp_ntz(9))) as string);
select cast(map('k', cast('2020-01-01 00:00:00.123456789' as timestamp_ntz(9))) as string);
select cast(named_struct('f', cast('2020-01-01 00:00:00.123456789' as timestamp_ltz(9))) as string);
-- NULL and a real string context.
select cast(cast(null as timestamp_ntz(9)) as string);
select concat('ts=', cast(cast('2020-01-01 00:00:00.123456789' as timestamp_ntz(9)) as string));

-- SPARK-57293: cast between nanosecond-precision timestamps and their microsecond counterparts.
-- Both directions stay within one zone family (pure representation conversions, no timezone
-- involvement); narrowing/round-trip yield micros.
-- TIMESTAMP_NTZ <-> TIMESTAMP_NTZ(p): widening sets the sub-microsecond part to 0.
select cast(cast('2020-01-01 00:00:00.123456' as timestamp_ntz) as timestamp_ntz(9));
-- Narrowing TIMESTAMP_NTZ(p) -> TIMESTAMP_NTZ drops the sub-microsecond digits (floor).
select cast(cast('2020-01-01 00:00:00.123456789' as timestamp_ntz(9)) as timestamp_ntz);
-- Round-trip micros -> nanos(p) -> micros returns the original microseconds.
select cast(cast(cast('2020-01-01 00:00:00.123456' as timestamp_ntz) as timestamp_ntz(9)) as timestamp_ntz);
-- TIMESTAMP_LTZ <-> TIMESTAMP_LTZ(p): same conversions on the epoch-micros instant.
select cast(cast('2020-01-01 00:00:00.123456' as timestamp) as timestamp_ltz(9));
select cast(cast('2020-01-01 00:00:00.123456789' as timestamp_ltz(9)) as timestamp);
select cast(cast(cast('2020-01-01 00:00:00.123456' as timestamp) as timestamp_ltz(9)) as timestamp);

-- SPARK-57490: cast between nanosecond timestamp types of different precision.
-- Keep SQL-layer coverage focused on parser / typed-literal / :: resolution paths.
-- Value semantics (flooring, widening, pre-epoch behavior, null propagation) are covered in
-- CastSuite* unit tests.
select typeof(timestamp_ntz'2020-01-01 00:00:00.123456789'::timestamp_ntz(7));
select typeof(timestamp_ltz'2020-01-01 00:00:00.123456789'::timestamp_ltz(8));
-- Exercise both typed literals and nested/chained :: casts in SQL text parsing.
select timestamp_ntz'2020-01-01 00:00:00.123456789'::timestamp_ntz(7);
select timestamp_ltz'2020-01-01 00:00:00.123456789'::timestamp_ltz(8);
select timestamp_ntz'2020-01-01 00:00:00.123456789'::timestamp_ntz(7)::timestamp_ntz(9);
select timestamp_ltz'1960-01-01 00:00:00.123456789'::timestamp_ltz(9)::timestamp_ltz(7);
select cast(null as timestamp_ntz(9))::timestamp_ntz(7);
select cast(null as timestamp_ltz(8))::timestamp_ltz(9);

-- Cross-family cast between nanosecond TIMESTAMP_LTZ(p) and TIMESTAMP_NTZ(q).
-- The conversion reinterprets the value against the session time zone, so SQL-layer coverage stays
-- zone-independent (type resolution, lossless same-zone round-trips, null propagation); the
-- zone-aware value semantics (flooring, pre-epoch, zone shifts) are covered in CastSuite* unit tests.
select typeof(timestamp_ltz'2020-01-01 00:00:00.123456789'::timestamp_ntz(7));
select typeof(timestamp_ltz'2020-01-01 00:00:00.123456789'::timestamp_ntz(9));
select typeof(timestamp_ntz'2020-01-01 00:00:00.123456789'::timestamp_ltz(8));
select typeof(timestamp_ntz'2020-01-01 00:00:00.123456789'::timestamp_ltz(9));
-- Same-zone round-trips are lossless at equal precision and re-floored when narrowing in between.
select timestamp_ltz'2020-01-01 00:00:00.123456789'::timestamp_ntz(9)::timestamp_ltz(9);
select timestamp_ntz'2020-01-01 00:00:00.123456789'::timestamp_ltz(9)::timestamp_ntz(9);
select timestamp_ltz'2020-01-01 00:00:00.123456789'::timestamp_ntz(7)::timestamp_ltz(7);
select timestamp_ntz'1960-01-01 00:00:00.123456789'::timestamp_ltz(7)::timestamp_ntz(7);
-- Null propagation in both directions.
select cast(null as timestamp_ltz(9))::timestamp_ntz(7);
select cast(null as timestamp_ntz(8))::timestamp_ltz(9);

-- Boundary at precision 6: TIMESTAMP_LTZ(6) is TIMESTAMP and TIMESTAMP_NTZ(6) is TIMESTAMP_NTZ, so
-- these cross-family casts mix the micro family member with the other family's nanos member.
select typeof(timestamp'2020-01-01 00:00:00.123456'::timestamp_ntz(9));
select typeof(timestamp_ntz'2020-01-01 00:00:00.123456'::timestamp_ltz(9));
select typeof(timestamp_ltz'2020-01-01 00:00:00.123456789'::timestamp_ntz(6));
select typeof(timestamp_ntz'2020-01-01 00:00:00.123456789'::timestamp_ltz(6));
-- Same-zone round-trips: micro(6) -> nanos -> micro(6) is lossless; nanos -> micro(6) -> nanos
-- drops the sub-microsecond digits.
select timestamp'2020-01-01 00:00:00.123456'::timestamp_ntz(9)::timestamp;
select timestamp_ntz'2020-01-01 00:00:00.123456'::timestamp_ltz(9)::timestamp_ntz;
select timestamp_ltz'2020-01-01 00:00:00.123456789'::timestamp_ntz(6)::timestamp_ltz;
select timestamp_ntz'2020-01-01 00:00:00.123456789'::timestamp_ltz(6)::timestamp_ntz;
-- Null propagation across the micro boundary in all four directions.
select cast(null as timestamp)::timestamp_ntz(9);
select cast(null as timestamp_ntz)::timestamp_ltz(9);
select cast(null as timestamp_ntz(9))::timestamp;
select cast(null as timestamp_ltz(9))::timestamp_ntz;

select cast(cast('inf' as double) as timestamp);
select cast(cast('inf' as float) as timestamp);

-- cast ANSI intervals to integrals
select cast(interval '1' year as tinyint);
select cast(interval '-10-2' year to month as smallint);
select cast(interval '1000' month as int);
select cast(interval -'10.123456' second as tinyint);
select cast(interval '23:59:59' hour to second as smallint);
select cast(interval -'1 02:03:04.123' day to second as int);
select cast(interval '10' day as bigint);

select cast(interval '-1000' month as tinyint);
select cast(interval '1000000' second as smallint);

-- cast integrals to ANSI intervals
select cast(1Y as interval year);
select cast(-122S as interval year to month);
select cast(ym as interval year to month) from values(-122S) as t(ym);
select cast(1000 as interval month);
select cast(-10L as interval second);
select cast(100Y as interval hour to second);
select cast(dt as interval hour to second) from values(100Y) as t(dt);
select cast(-1000S as interval day to second);
select cast(10 as interval day);

select cast(2147483647 as interval year);
select cast(-9223372036854775808L as interval day);

-- cast ANSI intervals to decimals
select cast(interval '-1' year as decimal(10, 0));
select cast(interval '1.000001' second as decimal(10, 6));
select cast(interval '08:11:10.001' hour to second as decimal(10, 4));
select cast(interval '1 01:02:03.1' day to second as decimal(8, 1));
select cast(interval '10.123' second as decimal(4, 2));
select cast(interval '10.005' second as decimal(4, 2));
select cast(interval '10.123' second as decimal(5, 2));
select cast(interval '10.123' second as decimal(1, 0));

-- cast decimals to ANSI intervals
select cast(10.123456BD as interval day to second);
select cast(80.654321BD as interval hour to minute);
select cast(-10.123456BD as interval year to month);
select cast(10.654321BD as interval month);

-- cast TIME to integral types
SELECT CAST(TIME '00:01:52' AS tinyint);
SELECT CAST(TIME '00:01:52' AS smallint);
SELECT CAST(TIME '00:01:52' AS int);
SELECT CAST(TIME '00:01:52' AS bigint);

-- cast TIME to integral types with potential overflow
SELECT CAST(TIME '23:59:59' AS tinyint);
SELECT CAST(TIME '23:59:59' AS smallint);
SELECT CAST(TIME '23:59:59' AS int);
SELECT CAST(TIME '23:59:59' AS bigint);

-- cast TIME with fractional seconds (should floor)
SELECT CAST(TIME '00:00:17.5' AS tinyint);
SELECT CAST(TIME '00:00:17.5' AS int);
SELECT CAST(TIME '00:00:17.9' AS int);

-- cast TIME edge cases
SELECT CAST(TIME '00:00:00' AS tinyint);
SELECT CAST(TIME '00:00:00' AS int);

-- cast double colon syntax tests
SELECT '1.23' :: int;
SELECT 'abc' :: int;
SELECT '12345678901234567890123' :: long;
SELECT '' :: int;
SELECT NULL :: int;
SELECT '123.a' :: int;
SELECT '-2147483648' :: int;
SELECT HEX('abc' :: binary);
SELECT HEX((123 :: byte) :: binary);
SELECT 'interval 3 month 1 hour' :: interval;
SELECT interval 3 day 1 second :: string;
select ' 1 ' :: DOUBLE;
select '1.0 ' :: DEC;
select '\t\t true \n\r ' :: boolean;
select '2022-01-01 00:00:00' :: timestamp;
select interval '-10-2' year to month :: smallint;
select -10L :: interval second;
select interval '08:11:10.001' hour to second :: decimal(10, 4);
select 10.123456BD :: interval day to second;

-- cast TIME using double colon syntax
SELECT TIME '00:01:52' :: tinyint;
SELECT TIME '00:01:52' :: int;
SELECT TIME '23:59:59' :: tinyint;
SELECT TIME '23:59:59' :: int;

SELECT '1.23' :: int :: long;
SELECT '2147483648' :: long :: int;
SELECT CAST('2147483648' :: long AS int);
SELECT map(1, '123', 2, '456')[1] :: int;

-- cast double colon syntax negative tests
SELECT '2147483648' :: BINT;
SELECT '2147483648' :: SELECT;
SELECT FALSE IS NOT NULL :: string;

-- SPARK-52619: cast TIME to DECIMAL with sufficient precision and scale.
SELECT CAST(time '00:00:00' AS decimal(1, 0));
SELECT CAST(time '12:00:00' AS decimal(7, 2));
SELECT CAST(time '01:30:45' AS decimal(8, 3));
SELECT CAST(time '23:59:59' AS decimal(9, 4));
SELECT CAST(time '01:02:03' AS decimal(15, 9));
SELECT CAST(time '10:20:30' AS decimal(20, 10));
SELECT CAST(time '23:59:59.001' AS decimal(8, 3));
SELECT CAST(time '23:59:59.999999' AS decimal(11, 6));
SELECT CAST(time '23:59:59.999999999' AS decimal(14, 9));
SELECT CAST(time '23:59:59.999999999' AS decimal(20, 10));

-- SPARK-52619: cast TIME to DECIMAL with insufficient precision.
SELECT CAST(time '00:01:00' AS decimal(1, 0));
SELECT CAST(time '01:00:00' AS decimal(3, 0));
SELECT CAST(time '10:00:00' AS decimal(5, 2));

-- SPARK-52619: cast TIME to DECIMAL with insufficient scale.
SELECT CAST(time '23:59:59.9' AS decimal(6, 0));
SELECT CAST(time '23:59:59.999' AS decimal(8, 2));
SELECT CAST(time '23:59:59.999999' AS decimal(11, 5));
SELECT CAST(time '23:59:59.999999999' AS decimal(14, 8));

-- SPARK-57618: cast TIMESTAMP_NTZ(q) to TIME(p) extracts the time-of-day and truncates to p.
-- This direction is deterministic and time-zone independent.
SELECT CAST(timestamp_ntz'2020-05-17 12:34:56.789012' AS TIME(6));
SELECT CAST(timestamp_ntz'2020-05-17 12:34:56.789012' AS TIME(3));
SELECT CAST(timestamp_ntz'2020-05-17 12:34:56.789012' AS TIME(0));
-- Pre-epoch wall-clock time-of-day is preserved.
SELECT CAST(timestamp_ntz'1969-12-31 23:59:59.123456' AS TIME(6));
-- Nanosecond TIMESTAMP_NTZ(q) preserves the sub-microsecond digits up to p.
SELECT CAST(timestamp_ntz'2020-05-17 12:34:56.789012345'::timestamp_ntz(9) AS TIME(9));
SELECT CAST(timestamp_ntz'2020-05-17 12:34:56.789012345'::timestamp_ntz(9) AS TIME(7));
SELECT CAST(timestamp_ntz'2020-05-17 12:34:56.789012345'::timestamp_ntz(9) AS TIME(6));
SELECT CAST(timestamp_ntz'2020-05-17 12:34:56.789012345'::timestamp_ntz(7) AS TIME(9));
-- Double colon syntax.
SELECT timestamp_ntz'2020-05-17 12:34:56.789012' :: TIME(6);

-- SPARK-57618: cast TIME(p) to TIMESTAMP_NTZ(q) takes the date from CURRENT_DATE. SQL-layer
-- coverage stays deterministic: type resolution, the date anchor equals current_date(), and the
-- value round-trips back to TIME. The current-date stabilization is covered by ComputeCurrentTimeSuite
-- and the value semantics by CastSuite* unit tests.
SELECT typeof(CAST(TIME'12:34:56' AS TIMESTAMP_NTZ));
SELECT typeof(CAST(TIME'12:34:56' AS TIMESTAMP_NTZ(9)));
-- The date fields come from the query current date.
SELECT CAST(CAST(TIME'12:34:56' AS TIMESTAMP_NTZ) AS DATE) = current_date();
SELECT CAST(CAST(TIME'12:34:56.789012345' AS TIMESTAMP_NTZ(9)) AS DATE) = current_date();
-- Round-trips re-extract the original time-of-day (truncated to the intermediate precision).
SELECT CAST(CAST(TIME'12:34:56.789012' AS TIMESTAMP_NTZ) AS TIME(6));
SELECT CAST(CAST(TIME'12:34:56.789012345' AS TIMESTAMP_NTZ(9)) AS TIME(9));
SELECT CAST(CAST(TIME'12:34:56.789012345' AS TIMESTAMP_NTZ(7)) AS TIME(9));
SELECT CAST(CAST(TIME'12:34:56.789012345'::time(9) AS TIMESTAMP_NTZ(6)) AS TIME(9));
-- Null propagation in both directions.
SELECT CAST(CAST(NULL AS TIME) AS TIMESTAMP_NTZ);
SELECT CAST(CAST(NULL AS TIME) AS TIMESTAMP_NTZ(9));
SELECT CAST(CAST(NULL AS TIMESTAMP_NTZ) AS TIME(6));
SELECT CAST(CAST(NULL AS TIMESTAMP_NTZ(9)) AS TIME(6));

-- SPARK-57749: inside an inline table the TIME -> TIMESTAMP_NTZ cast is foldable and carries no
-- CURRENT_LIKE pattern, so it is early-evaluated at analysis time and is NOT stabilized by
-- ComputeCurrentTime. Its date anchor is LocalDate.now() at analysis (not necessarily
-- current_date()), so it is intentionally not compared against current_date() here -- that would be
-- midnight-sensitive. Coverage stays deterministic via the round-trip back to TIME: the date
-- cancels, so only the time-of-day is asserted.
SELECT CAST(x AS TIME(6)) FROM VALUES (CAST(TIME'12:34:56.789012' AS TIMESTAMP_NTZ)) t(x);
SELECT CAST(x AS TIME(9)) FROM VALUES (CAST(TIME'12:34:56.789012345' AS TIMESTAMP_NTZ(9))) t(x);

-- SPARK-57618: TIME and TIMESTAMP_NTZ have no common type, so implicit coercion is rejected; an
-- explicit CAST (as above) is required. This guards `findWiderDateTimeType` against ever widening
-- TIME, which would silently inject CURRENT_DATE into UNION / coalesce / CASE.
SELECT time'12:34:56' UNION ALL SELECT timestamp_ntz'2020-05-17 12:34:56';
SELECT coalesce(time'12:34:56', timestamp_ntz'2020-05-17 12:34:56');
SELECT coalesce(time'12:34:56', timestamp_ntz'2020-05-17 12:34:56.789012345'::timestamp_ntz(9));
SELECT CASE WHEN true THEN time'12:34:56' ELSE timestamp_ntz'2020-05-17 12:34:56' END;

-- SPARK-57660: cast TIMESTAMP_LTZ(q) to TIME(p) extracts the local wall-clock time-of-day in the
-- session time zone and truncates to p. The LTZ literal is parsed in the session zone and the
-- time-of-day is read back in the same zone, so these values stay zone-independent.
SELECT CAST(timestamp'2020-05-17 12:34:56.789012' AS TIME(6));
SELECT CAST(timestamp'2020-05-17 12:34:56.789012' AS TIME(3));
SELECT CAST(timestamp'2020-05-17 12:34:56.789012' AS TIME(0));
-- Nanosecond TIMESTAMP_LTZ(q) preserves the sub-microsecond digits up to p.
SELECT CAST(timestamp_ltz'2020-05-17 12:34:56.789012345'::timestamp_ltz(9) AS TIME(9));
SELECT CAST(timestamp_ltz'2020-05-17 12:34:56.789012345'::timestamp_ltz(9) AS TIME(7));
SELECT CAST(timestamp_ltz'2020-05-17 12:34:56.789012345'::timestamp_ltz(9) AS TIME(6));
SELECT CAST(timestamp_ltz'2020-05-17 12:34:56.789012345'::timestamp_ltz(7) AS TIME(9));
-- Double colon syntax.
SELECT timestamp'2020-05-17 12:34:56.789012' :: TIME(6);

-- SPARK-57660: cast TIME(p) to TIMESTAMP_LTZ(q) takes the date from CURRENT_DATE and interprets the
-- result in the session time zone. SQL-layer coverage stays deterministic: type resolution, the date
-- anchor equals current_date(), and the value round-trips back to TIME. The current-date
-- stabilization is covered by ComputeCurrentTimeSuite and the value semantics by CastSuite* tests.
SELECT typeof(CAST(TIME'12:34:56' AS TIMESTAMP_LTZ));
SELECT typeof(CAST(TIME'12:34:56' AS TIMESTAMP_LTZ(9)));
-- The date fields come from the query current date.
SELECT CAST(CAST(TIME'12:34:56' AS TIMESTAMP_LTZ) AS DATE) = current_date();
SELECT CAST(CAST(TIME'12:34:56.789012345' AS TIMESTAMP_LTZ(9)) AS DATE) = current_date();
-- Round-trips re-extract the original time-of-day (truncated to the intermediate precision).
SELECT CAST(CAST(TIME'12:34:56.789012' AS TIMESTAMP_LTZ) AS TIME(6));
SELECT CAST(CAST(TIME'12:34:56.789012345' AS TIMESTAMP_LTZ(9)) AS TIME(9));
SELECT CAST(CAST(TIME'12:34:56.789012345' AS TIMESTAMP_LTZ(7)) AS TIME(9));
SELECT CAST(CAST(TIME'12:34:56.789012345'::time(9) AS TIMESTAMP_LTZ(6)) AS TIME(9));
-- Null propagation in both directions.
SELECT CAST(CAST(NULL AS TIME) AS TIMESTAMP_LTZ);
SELECT CAST(CAST(NULL AS TIME) AS TIMESTAMP_LTZ(9));
SELECT CAST(CAST(NULL AS TIMESTAMP_LTZ) AS TIME(6));
SELECT CAST(CAST(NULL AS TIMESTAMP_LTZ(9)) AS TIME(6));

-- SPARK-57749: inside an inline table the TIME -> TIMESTAMP_LTZ cast is foldable and carries no
-- CURRENT_LIKE pattern, so it is early-evaluated at analysis time and is NOT stabilized by
-- ComputeCurrentTime. Its date anchor is LocalDate.now() at analysis (not necessarily
-- current_date()), so it is intentionally not compared against current_date() here -- that would be
-- midnight-sensitive. Coverage stays deterministic via the round-trip back to TIME: the date
-- cancels, so only the time-of-day is asserted.
SELECT CAST(x AS TIME(6)) FROM VALUES (CAST(TIME'12:34:56.789012' AS TIMESTAMP_LTZ)) t(x);
SELECT CAST(x AS TIME(9)) FROM VALUES (CAST(TIME'12:34:56.789012345' AS TIMESTAMP_LTZ(9))) t(x);

-- SPARK-57660: TIME and TIMESTAMP_LTZ have no common type, so implicit coercion is rejected; an
-- explicit CAST (as above) is required. This guards `findWiderDateTimeType` against ever widening
-- TIME, which would silently inject CURRENT_DATE into UNION / coalesce / CASE.
SELECT time'12:34:56' UNION ALL SELECT timestamp'2020-05-17 12:34:56';
SELECT coalesce(time'12:34:56', timestamp'2020-05-17 12:34:56');
SELECT coalesce(time'12:34:56', timestamp_ltz'2020-05-17 12:34:56.789012345'::timestamp_ltz(9));
SELECT CASE WHEN true THEN time'12:34:56' ELSE timestamp'2020-05-17 12:34:56' END;
