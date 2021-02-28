--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- TIMESTAMP
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/timestamp.sql

CREATE TABLE TIMESTAMP_TBL (d1 timestamp) USING parquet;

-- Test shorthand input values
-- We can't just "select" the results since they aren't constants; test for
-- equality instead.  We can do that by running the test inside a transaction
-- block, within which the value of 'now' shouldn't change.  We also check
-- that 'now' *does* change over a reasonable interval such as 100 msec.
-- NOTE: it is possible for this part of the test to fail if the transaction
-- block is entered exactly at local midnight; then 'now' and 'today' have
-- the same values and the counts will come out different.

-- PostgreSQL implicitly casts string literals to data with timestamp types, but
-- Spark does not support that kind of implicit casts.
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('now'));
-- SELECT pg_sleep(0.1);

-- BEGIN;

-- PostgreSQL implicitly casts string literals to data with timestamp types, but
-- Spark does not support that kind of implicit casts.
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('now'));
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('today'));
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('yesterday'));
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('tomorrow'));
-- time zone should be ignored by this data type
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('tomorrow EST'));
-- [SPARK-29024] Ignore case while resolving time zones
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('tomorrow Zulu'));

SELECT count(*) AS One FROM TIMESTAMP_TBL WHERE d1 = timestamp 'today';
SELECT count(*) AS Three FROM TIMESTAMP_TBL WHERE d1 = timestamp 'tomorrow';
SELECT count(*) AS One FROM TIMESTAMP_TBL WHERE d1 = timestamp 'yesterday';
-- [SPARK-29025] Support seconds precision by the timestamp type
-- SELECT count(*) AS One FROM TIMESTAMP_TBL WHERE d1 = timestamp(2) 'now';

-- COMMIT;

-- DELETE FROM TIMESTAMP_TBL;

-- verify uniform transaction time within transaction block
-- BEGIN;
-- INSERT INTO TIMESTAMP_TBL VALUES ('now');
-- SELECT pg_sleep(0.1);
-- INSERT INTO TIMESTAMP_TBL VALUES ('now');
-- SELECT pg_sleep(0.1);
-- SELECT count(*) AS two FROM TIMESTAMP_TBL WHERE d1 = timestamp(2) 'now';
-- COMMIT;

TRUNCATE TABLE TIMESTAMP_TBL;

-- Special values
-- INSERT INTO TIMESTAMP_TBL VALUES ('-infinity');
-- INSERT INTO TIMESTAMP_TBL VALUES ('infinity');
-- PostgreSQL implicitly casts string literals to data with timestamp types, but
-- Spark does not support that kind of implicit casts.
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('epoch'));
-- [SPARK-27923] Spark SQL insert there obsolete special values to NULL
-- Obsolete special values
-- INSERT INTO TIMESTAMP_TBL VALUES ('invalid');
-- INSERT INTO TIMESTAMP_TBL VALUES ('undefined');
-- INSERT INTO TIMESTAMP_TBL VALUES ('current');

-- [SPARK-28259] Date/Time Output Styles and Date Order Conventions
-- Postgres v6.0 standard output format
-- INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01 1997 PST');

-- Variations on Postgres v6.1 standard output format
-- INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.000001 1997 PST');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.999999 1997 PST');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.4 1997 PST');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.5 1997 PST');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.6 1997 PST');

-- ISO 8601 format
-- PostgreSQL implicitly casts string literals to data with timestamp types, but
-- Spark does not support that kind of implicit casts.
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('1997-01-02'));
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('1997-01-02 03:04:05'));
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('1997-02-10 17:32:01-08'));
-- INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01-0800');
-- INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01 -08:00');
-- INSERT INTO TIMESTAMP_TBL VALUES ('19970210 173201 -0800');
-- INSERT INTO TIMESTAMP_TBL VALUES ('1997-06-10 17:32:01 -07:00');
INSERT INTO TIMESTAMP_TBL VALUES (timestamp('2001-09-22T18:19:20'));

-- POSIX format (note that the timezone abbrev is just decoration here)
-- INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 08:14:01 GMT+8');
-- INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 13:14:02 GMT-1');
-- INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 12:14:03 GMT-2');
-- INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 03:14:04 PST+8');
-- INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 02:14:05 MST+7:00');

-- Variations for acceptable input formats
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 17:32:01 1997 -0800');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 5:32PM 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('1997/02/10 17:32:01-0800');
-- INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01 PST');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb-10-1997 17:32:01 PST');
-- INSERT INTO TIMESTAMP_TBL VALUES ('02-10-1997 17:32:01 PST');
-- INSERT INTO TIMESTAMP_TBL VALUES ('19970210 173201 PST');
-- set datestyle to ymd;
-- INSERT INTO TIMESTAMP_TBL VALUES ('97FEB10 5:32:01PM UTC');
-- INSERT INTO TIMESTAMP_TBL VALUES ('97/02/10 17:32:01 UTC');
-- reset datestyle;
-- INSERT INTO TIMESTAMP_TBL VALUES ('1997.041 17:32:01 UTC');
-- INSERT INTO TIMESTAMP_TBL VALUES ('19970210 173201 America/New_York');
-- this fails (even though TZ is a no-op, we still look it up)
-- INSERT INTO TIMESTAMP_TBL VALUES ('19970710 173201 America/Does_not_exist');

-- Check date conversion and date arithmetic
-- INSERT INTO TIMESTAMP_TBL VALUES ('1997-06-10 18:32:01 PDT');

-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 11 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 12 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 13 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 14 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 15 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1997');

-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 0097 BC');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 0097');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 0597');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1097');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1697');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1797');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1897');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 2097');

-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 28 17:32:01 1996');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 29 17:32:01 1996');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Mar 01 17:32:01 1996');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Dec 30 17:32:01 1996');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 1996');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Jan 01 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 28 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 29 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Mar 01 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Dec 30 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 1997');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 1999');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Jan 01 17:32:01 2000');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 2000');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Jan 01 17:32:01 2001');

-- Currently unsupported syntax and ranges
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 -0097');
-- INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 5097 BC');

SELECT '' AS `64`, d1 FROM TIMESTAMP_TBL;

-- [SPARK-28253] Date/Timestamp type have different low value and high value with Spark
-- Check behavior at the lower boundary of the timestamp range
-- SELECT '4714-11-24 00:00:00 BC'::timestamp;
-- SELECT '4714-11-23 23:59:59 BC'::timestamp;  -- out of range
-- The upper boundary differs between integer and float timestamps, so no check

-- Demonstrate functions and operators
SELECT '' AS `48`, d1 FROM TIMESTAMP_TBL
   WHERE d1 > timestamp '1997-01-02';

SELECT '' AS `15`, d1 FROM TIMESTAMP_TBL
   WHERE d1 < timestamp '1997-01-02';

SELECT '' AS one, d1 FROM TIMESTAMP_TBL
   WHERE d1 = timestamp '1997-01-02';

SELECT '' AS `63`, d1 FROM TIMESTAMP_TBL
   WHERE d1 != timestamp '1997-01-02';

SELECT '' AS `16`, d1 FROM TIMESTAMP_TBL
   WHERE d1 <= timestamp '1997-01-02';

SELECT '' AS `49`, d1 FROM TIMESTAMP_TBL
   WHERE d1 >= timestamp '1997-01-02';

SELECT '' AS `54`, d1 - timestamp '1997-01-02' AS diff
   FROM TIMESTAMP_TBL WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01';

SELECT '' AS date_trunc_week, date_trunc( 'week', timestamp '2004-02-29 15:44:17.71393' ) AS week_trunc;

-- Test casting within a BETWEEN qualifier
SELECT '' AS `54`, d1 - timestamp '1997-01-02' AS diff
  FROM TIMESTAMP_TBL
  WHERE d1 BETWEEN timestamp '1902-01-01'
   AND timestamp '2038-01-01';

SELECT '' AS `54`, d1 as `timestamp`,
    date_part( 'year', d1) AS `year`, date_part( 'month', d1) AS `month`,
    date_part( 'day', d1) AS `day`, date_part( 'hour', d1) AS `hour`,
    date_part( 'minute', d1) AS `minute`, date_part( 'second', d1) AS `second`
    FROM TIMESTAMP_TBL WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01';

-- SELECT '' AS `54`, d1 as `timestamp`,
--     date_part( 'quarter', d1) AS quarter, date_part( 'msec', d1) AS msec,
--     date_part( 'usec', d1) AS usec
--     FROM TIMESTAMP_TBL WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01';

-- SELECT '' AS `54`, d1 as `timestamp`,
--     date_part( 'isoyear', d1) AS isoyear, date_part( 'week', d1) AS week,
--     date_part( 'dow', d1) AS dow
--     FROM TIMESTAMP_TBL WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01';

-- [SPARK-28137] Data Type Formatting Functions
-- TO_CHAR()
-- SELECT '' AS to_char_1, to_char(d1, 'DAY Day day DY Dy dy MONTH Month month RM MON Mon mon')
--    FROM TIMESTAMP_TBL;

-- SELECT '' AS to_char_2, to_char(d1, 'FMDAY FMDay FMday FMMONTH FMMonth FMmonth FMRM')
--    FROM TIMESTAMP_TBL;

-- SELECT '' AS to_char_3, to_char(d1, 'Y,YYY YYYY YYY YY Y CC Q MM WW DDD DD D J')
--    FROM TIMESTAMP_TBL;

-- SELECT '' AS to_char_4, to_char(d1, 'FMY,YYY FMYYYY FMYYY FMYY FMY FMCC FMQ FMMM FMWW FMDDD FMDD FMD FMJ')
--    FROM TIMESTAMP_TBL;

-- SELECT '' AS to_char_5, to_char(d1, 'HH HH12 HH24 MI SS SSSS')
--    FROM TIMESTAMP_TBL;

-- SELECT '' AS to_char_6, to_char(d1, E'"HH:MI:SS is" HH:MI:SS "\\"text between quote marks\\""')
--    FROM TIMESTAMP_TBL;

-- SELECT '' AS to_char_7, to_char(d1, 'HH24--text--MI--text--SS')
--    FROM TIMESTAMP_TBL;

-- SELECT '' AS to_char_8, to_char(d1, 'YYYYTH YYYYth Jth')
--    FROM TIMESTAMP_TBL;

-- SELECT '' AS to_char_9, to_char(d1, 'YYYY A.D. YYYY a.d. YYYY bc HH:MI:SS P.M. HH:MI:SS p.m. HH:MI:SS pm')
--    FROM TIMESTAMP_TBL;

-- SELECT '' AS to_char_10, to_char(d1, 'IYYY IYY IY I IW IDDD ID')
--    FROM TIMESTAMP_TBL;

-- SELECT '' AS to_char_11, to_char(d1, 'FMIYYY FMIYY FMIY FMI FMIW FMIDDD FMID')
--    FROM TIMESTAMP_TBL;


-- timestamp numeric fields constructor
SELECT make_timestamp(2014,12,28,6,30,45.887);

DROP TABLE TIMESTAMP_TBL;
