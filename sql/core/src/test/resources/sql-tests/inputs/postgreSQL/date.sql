--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- DATE
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/date.sql

CREATE TABLE DATE_TBL (f1 date) USING parquet;

-- PostgreSQL implicitly casts string literals to data with date types, but
-- Spark does not support that kind of implicit casts.
INSERT INTO DATE_TBL VALUES (date('1957-04-09'));
INSERT INTO DATE_TBL VALUES (date('1957-06-13'));
INSERT INTO DATE_TBL VALUES (date('1996-02-28'));
INSERT INTO DATE_TBL VALUES (date('1996-02-29'));
INSERT INTO DATE_TBL VALUES (date('1996-03-01'));
INSERT INTO DATE_TBL VALUES (date('1996-03-02'));
INSERT INTO DATE_TBL VALUES (date('1997-02-28'));
-- [SPARK-27923] Skip invalid date: 1997-02-29
-- INSERT INTO DATE_TBL VALUES ('1997-02-29'));
INSERT INTO DATE_TBL VALUES (date('1997-03-01'));
INSERT INTO DATE_TBL VALUES (date('1997-03-02'));
INSERT INTO DATE_TBL VALUES (date('2000-04-01'));
INSERT INTO DATE_TBL VALUES (date('2000-04-02'));
INSERT INTO DATE_TBL VALUES (date('2000-04-03'));
INSERT INTO DATE_TBL VALUES (date('2038-04-08'));
INSERT INTO DATE_TBL VALUES (date('2039-04-09'));
INSERT INTO DATE_TBL VALUES (date('2040-04-10'));

SELECT f1 AS `Fifteen` FROM DATE_TBL;

SELECT f1 AS `Nine` FROM DATE_TBL WHERE f1 < '2000-01-01';

SELECT f1 AS `Three` FROM DATE_TBL
  WHERE f1 BETWEEN '2000-01-01' AND '2001-01-01';

-- Skip the formats that we do not supported. Please check [SPARK-8995] for all supported formats
--
-- Check all the documented input formats
--
-- [SPARK-28259] Date/Time Output Styles and Date Order Conventions
-- SET datestyle TO iso;  -- display results in ISO

-- SET datestyle TO ymd;

-- SELECT date 'January 8, 1999';
SELECT date '1999-01-08';
SELECT date '1999-01-18';
-- SELECT date '1/8/1999';
-- SELECT date '1/18/1999';
-- SELECT date '18/1/1999';
-- SELECT date '01/02/03';
-- SELECT date '19990108';
-- SELECT date '990108';
-- SELECT date '1999.008';
-- SELECT date 'J2451187';
-- SELECT date 'January 8, 99 BC';

-- SELECT date '99-Jan-08';
-- SELECT date '1999-Jan-08';
-- SELECT date '08-Jan-99';
-- SELECT date '08-Jan-1999';
-- SELECT date 'Jan-08-99';
-- SELECT date 'Jan-08-1999';
-- SELECT date '99-08-Jan';
-- SELECT date '1999-08-Jan';

-- SELECT date '99 Jan 08';
SELECT date '1999 Jan 08';
-- SELECT date '08 Jan 99';
-- SELECT date '08 Jan 1999';
-- SELECT date 'Jan 08 99';
-- SELECT date 'Jan 08 1999';
-- SELECT date '99 08 Jan';
SELECT date '1999 08 Jan';

-- SELECT date '99-01-08';
SELECT date '1999-01-08';
-- SELECT date '08-01-99';
-- SELECT date '08-01-1999';
-- SELECT date '01-08-99';
-- SELECT date '01-08-1999';
-- SELECT date '99-08-01';
SELECT date '1999-08-01';

-- SELECT date '99 01 08';
SELECT date '1999 01 08';
-- SELECT date '08 01 99';
-- SELECT date '08 01 1999';
-- SELECT date '01 08 99';
-- SELECT date '01 08 1999';
-- SELECT date '99 08 01';
SELECT date '1999 08 01';

-- SET datestyle TO dmy;

-- SELECT date 'January 8, 1999';
SELECT date '1999-01-08';
-- SELECT date '1999-01-18';
-- SELECT date '1/8/1999';
-- SELECT date '1/18/1999';
-- SELECT date '18/1/1999';
-- SELECT date '01/02/03';
-- SELECT date '19990108';
-- SELECT date '990108';
-- SELECT date '1999.008';
-- SELECT date 'J2451187';
-- SELECT date 'January 8, 99 BC';

-- SELECT date '99-Jan-08';
-- SELECT date '1999-Jan-08';
-- SELECT date '08-Jan-99';
-- SELECT date '08-Jan-1999';
-- SELECT date 'Jan-08-99';
-- SELECT date 'Jan-08-1999';
-- SELECT date '99-08-Jan';
-- SELECT date '1999-08-Jan';

-- SELECT date '99 Jan 08';
SELECT date '1999 Jan 08';
-- SELECT date '08 Jan 99';
-- SELECT date '08 Jan 1999';
-- SELECT date 'Jan 08 99';
-- SELECT date 'Jan 08 1999';
-- SELECT date '99 08 Jan';
SELECT date '1999 08 Jan';

-- SELECT date '99-01-08';
SELECT date '1999-01-08';
-- SELECT date '08-01-99';
-- SELECT date '08-01-1999';
-- SELECT date '01-08-99';
-- SELECT date '01-08-1999';
-- SELECT date '99-08-01';
SELECT date '1999-08-01';

-- SELECT date '99 01 08';
SELECT date '1999 01 08';
-- SELECT date '08 01 99';
-- SELECT date '08 01 1999';
-- SELECT date '01 08 99';
-- SELECT date '01 08 1999';
-- SELECT date '99 08 01';
SELECT date '1999 08 01';

-- SET datestyle TO mdy;

-- SELECT date 'January 8, 1999';
SELECT date '1999-01-08';
SELECT date '1999-01-18';
-- SELECT date '1/8/1999';
-- SELECT date '1/18/1999';
-- SELECT date '18/1/1999';
-- SELECT date '01/02/03';
-- SELECT date '19990108';
-- SELECT date '990108';
-- SELECT date '1999.008';
-- SELECT date 'J2451187';
-- SELECT date 'January 8, 99 BC';

-- SELECT date '99-Jan-08';
-- SELECT date '1999-Jan-08';
-- SELECT date '08-Jan-99';
-- SELECT date '08-Jan-1999';
-- SELECT date 'Jan-08-99';
-- SELECT date 'Jan-08-1999';
-- SELECT date '99-08-Jan';
-- SELECT date '1999-08-Jan';

-- SELECT date '99 Jan 08';
SELECT date '1999 Jan 08';
-- SELECT date '08 Jan 99';
-- SELECT date '08 Jan 1999';
-- SELECT date 'Jan 08 99';
-- SELECT date 'Jan 08 1999';
-- SELECT date '99 08 Jan';
SELECT date '1999 08 Jan';

-- SELECT date '99-01-08';
SELECT date '1999-01-08';
-- SELECT date '08-01-99';
-- SELECT date '08-01-1999';
-- SELECT date '01-08-99';
-- SELECT date '01-08-1999';
-- SELECT date '99-08-01';
SELECT date '1999-08-01';

-- SELECT date '99 01 08';
SELECT date '1999 01 08';
-- SELECT date '08 01 99';
-- SELECT date '08 01 1999';
-- SELECT date '01 08 99';
-- SELECT date '01 08 1999';
-- SELECT date '99 08 01';
SELECT date '1999 08 01';

-- [SPARK-28253] Date type have different low value and high value
-- Check upper and lower limits of date range
SELECT date '4714-11-24 BC';
SELECT date '4714-11-23 BC';  -- out of range
SELECT date '5874897-12-31';
SELECT date '5874898-01-01';  -- out of range

-- RESET datestyle;

--
-- Simple math
-- Leave most of it for the horology tests
--

SELECT f1 - date '2000-01-01' AS `Days From 2K` FROM DATE_TBL;

SELECT f1 - date 'epoch' AS `Days From Epoch` FROM DATE_TBL;

SELECT date 'yesterday' - date 'today' AS `One day`;

SELECT date 'today' - date 'tomorrow' AS `One day`;

SELECT date 'yesterday' - date 'tomorrow' AS `Two days`;

SELECT date 'tomorrow' - date 'today' AS `One day`;

SELECT date 'today' - date 'yesterday' AS `One day`;

SELECT date 'tomorrow' - date 'yesterday' AS `Two days`;

-- [SPARK-28017] Enhance date EXTRACT
--
-- test extract!
--
-- epoch
--
-- SELECT EXTRACT(EPOCH FROM DATE        '1970-01-01');     --  0
-- SELECT EXTRACT(EPOCH FROM TIMESTAMP   '1970-01-01');     --  0
-- SELECT EXTRACT(EPOCH FROM TIMESTAMPTZ '1970-01-01+00');  --  0
--
-- century
--
-- SELECT EXTRACT(CENTURY FROM TO_DATE('0101-12-31 BC', 'yyyy-MM-dd G')); -- -2
-- SELECT EXTRACT(CENTURY FROM TO_DATE('0100-12-31 BC', 'yyyy-MM-dd G')); -- -1
-- SELECT EXTRACT(CENTURY FROM TO_DATE('0001-12-31 BC', 'yyyy-MM-dd G')); -- -1
-- SELECT EXTRACT(CENTURY FROM DATE '0001-01-01');    --  1
-- SELECT EXTRACT(CENTURY FROM DATE '0001-01-01 AD'); --  1
-- SELECT EXTRACT(CENTURY FROM DATE '1900-12-31');    -- 19
-- SELECT EXTRACT(CENTURY FROM DATE '1901-01-01');    -- 20
-- SELECT EXTRACT(CENTURY FROM DATE '2000-12-31');    -- 20
-- SELECT EXTRACT(CENTURY FROM DATE '2001-01-01');    -- 21
-- SELECT EXTRACT(CENTURY FROM CURRENT_DATE)>=21 AS True;     -- true
--
-- millennium
--
-- SELECT EXTRACT(MILLENNIUM FROM TO_DATE('0001-12-31 BC', 'yyyy-MM-dd G')); -- -1
-- SELECT EXTRACT(MILLENNIUM FROM DATE '0001-01-01 AD'); --  1
-- SELECT EXTRACT(MILLENNIUM FROM DATE '1000-12-31');    --  1
-- SELECT EXTRACT(MILLENNIUM FROM DATE '1001-01-01');    --  2
-- SELECT EXTRACT(MILLENNIUM FROM DATE '2000-12-31');    --  2
-- SELECT EXTRACT(MILLENNIUM FROM DATE '2001-01-01');    --  3
-- next test to be fixed on the turn of the next millennium;-)
-- SELECT EXTRACT(MILLENNIUM FROM CURRENT_DATE);         --  3
--
-- decade
--
-- SELECT EXTRACT(DECADE FROM DATE '1994-12-25');    -- 199
-- SELECT EXTRACT(DECADE FROM DATE '0010-01-01');    --   1
-- SELECT EXTRACT(DECADE FROM DATE '0009-12-31');    --   0
-- SELECT EXTRACT(DECADE FROM TO_DATE('0001-01-01 BC', 'yyyy-MM-dd G')); --   0
-- SELECT EXTRACT(DECADE FROM TO_DATE('0002-12-31 BC', 'yyyy-MM-dd G')); --  -1
-- SELECT EXTRACT(DECADE FROM TO_DATE('0011-01-01 BC', 'yyyy-MM-dd G')); --  -1
-- SELECT EXTRACT(DECADE FROM TO_DATE('0012-12-31 BC', 'yyyy-MM-dd G')); --  -2
--
-- some other types:
--
-- on a timestamp.
-- SELECT EXTRACT(CENTURY FROM NOW())>=21 AS True;       -- true
-- SELECT EXTRACT(CENTURY FROM TIMESTAMP '1970-03-20 04:30:00.00000'); -- 20
-- on an interval
-- SELECT EXTRACT(CENTURY FROM INTERVAL '100 y');  -- 1
-- SELECT EXTRACT(CENTURY FROM INTERVAL '99 y');   -- 0
-- SELECT EXTRACT(CENTURY FROM INTERVAL '-99 y');  -- 0
-- SELECT EXTRACT(CENTURY FROM INTERVAL '-100 y'); -- -1
--
-- test trunc function!
-- SELECT DATE_TRUNC('MILLENNIUM', TIMESTAMP '1970-03-20 04:30:00.00000'); -- 1001
-- SELECT DATE_TRUNC('MILLENNIUM', DATE '1970-03-20'); -- 1001-01-01
-- SELECT DATE_TRUNC('CENTURY', TIMESTAMP '1970-03-20 04:30:00.00000'); -- 1901
-- SELECT DATE_TRUNC('CENTURY', DATE '1970-03-20'); -- 1901
-- SELECT DATE_TRUNC('CENTURY', DATE '2004-08-10'); -- 2001-01-01
-- SELECT DATE_TRUNC('CENTURY', DATE '0002-02-04'); -- 0001-01-01
-- SELECT DATE_TRUNC('CENTURY', TO_DATE('0055-08-10 BC', 'yyyy-MM-dd G')); -- 0100-01-01 BC
-- SELECT DATE_TRUNC('DECADE', DATE '1993-12-25'); -- 1990-01-01
-- SELECT DATE_TRUNC('DECADE', DATE '0004-12-25'); -- 0001-01-01 BC
-- SELECT DATE_TRUNC('DECADE', TO_DATE('0002-12-31 BC', 'yyyy-MM-dd G')); -- 0011-01-01 BC

-- [SPARK-29006] Support special date/timestamp values `infinity`/`-infinity`
--
-- test infinity
--
-- select 'infinity'::date, '-infinity'::date;
-- select 'infinity'::date > 'today'::date as t;
-- select '-infinity'::date < 'today'::date as t;
-- select isfinite('infinity'::date), isfinite('-infinity'::date), isfinite('today'::date);
--
-- oscillating fields from non-finite date/timestamptz:
--
-- SELECT EXTRACT(HOUR FROM DATE 'infinity');      -- NULL
-- SELECT EXTRACT(HOUR FROM DATE '-infinity');     -- NULL
-- SELECT EXTRACT(HOUR FROM TIMESTAMP   'infinity');      -- NULL
-- SELECT EXTRACT(HOUR FROM TIMESTAMP   '-infinity');     -- NULL
-- SELECT EXTRACT(HOUR FROM TIMESTAMPTZ 'infinity');      -- NULL
-- SELECT EXTRACT(HOUR FROM TIMESTAMPTZ '-infinity');     -- NULL
-- all possible fields
-- SELECT EXTRACT(MICROSECONDS  FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(MILLISECONDS  FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(SECOND        FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(MINUTE        FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(HOUR          FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(DAY           FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(MONTH         FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(QUARTER       FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(WEEK          FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(DOW           FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(ISODOW        FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(DOY           FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(TIMEZONE      FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(TIMEZONE_M    FROM DATE 'infinity');    -- NULL
-- SELECT EXTRACT(TIMEZONE_H    FROM DATE 'infinity');    -- NULL
--
-- monotonic fields from non-finite date/timestamptz:
--
-- SELECT EXTRACT(EPOCH FROM DATE 'infinity');         --  Infinity
-- SELECT EXTRACT(EPOCH FROM DATE '-infinity');        -- -Infinity
-- SELECT EXTRACT(EPOCH FROM TIMESTAMP   'infinity');  --  Infinity
-- SELECT EXTRACT(EPOCH FROM TIMESTAMP   '-infinity'); -- -Infinity
-- SELECT EXTRACT(EPOCH FROM TIMESTAMPTZ 'infinity');  --  Infinity
-- SELECT EXTRACT(EPOCH FROM TIMESTAMPTZ '-infinity'); -- -Infinity
-- all possible fields
-- SELECT EXTRACT(YEAR       FROM DATE 'infinity');    --  Infinity
-- SELECT EXTRACT(DECADE     FROM DATE 'infinity');    --  Infinity
-- SELECT EXTRACT(CENTURY    FROM DATE 'infinity');    --  Infinity
-- SELECT EXTRACT(MILLENNIUM FROM DATE 'infinity');    --  Infinity
-- SELECT EXTRACT(JULIAN     FROM DATE 'infinity');    --  Infinity
-- SELECT EXTRACT(ISOYEAR    FROM DATE 'infinity');    --  Infinity
-- SELECT EXTRACT(EPOCH      FROM DATE 'infinity');    --  Infinity
--
-- wrong fields from non-finite date:
--
-- SELECT EXTRACT(MICROSEC  FROM DATE 'infinity');     -- ERROR:  timestamp units "microsec" not recognized
-- SELECT EXTRACT(UNDEFINED FROM DATE 'infinity');     -- ERROR:  timestamp units "undefined" not supported

-- test constructors
select make_date(2013, 7, 15);
-- [SPARK-28471] Formatting dates with negative years
select make_date(-44, 3, 15);
-- select make_time(8, 20, 0.0);
-- should fail
select make_date(2013, 2, 30);
select make_date(2013, 13, 1);
select make_date(2013, 11, -1);
-- select make_time(10, 55, 100.1);
-- select make_time(24, 0, 2.1);

DROP TABLE DATE_TBL;
