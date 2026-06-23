--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- TIME
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/time.sql

-- Spark uses microsecond precision (time(6)); the `time()` function casts to
-- the TIME type. PostgreSQL implicitly casts string literals to time, but Spark
-- does not support that kind of implicit cast, so the inserts cast explicitly.
CREATE TABLE TIME_TBL (f1 time(6)) USING parquet;

INSERT INTO TIME_TBL VALUES (time('00:00'));
INSERT INTO TIME_TBL VALUES (time('01:00'));
-- Spark's TIME type carries no time zone; PostgreSQL accepts and ignores one.
-- INSERT INTO TIME_TBL VALUES (time('02:03 PST'));
-- INSERT INTO TIME_TBL VALUES (time('11:59 EDT'));
INSERT INTO TIME_TBL VALUES (time('12:00'));
INSERT INTO TIME_TBL VALUES (time('12:01'));
INSERT INTO TIME_TBL VALUES (time('23:59'));
INSERT INTO TIME_TBL VALUES (time('23:59:59.99'));
-- Spark cannot extract a time-of-day from a timestamp + time zone string.
-- INSERT INTO TIME_TBL VALUES (time('2003-03-07 15:36:39 America/New_York'));
-- INSERT INTO TIME_TBL VALUES (time('2003-07-07 15:36:39 America/New_York'));

SELECT f1 AS `Time` FROM TIME_TBL;

SELECT f1 AS `Three` FROM TIME_TBL WHERE f1 < '05:06:07';

SELECT f1 AS `Five` FROM TIME_TBL WHERE f1 > '05:06:07';

SELECT f1 AS `None` FROM TIME_TBL WHERE f1 < '00:00';

SELECT f1 AS `Eight` FROM TIME_TBL WHERE f1 >= '00:00';

--
-- TIME simple math
--
-- We make a distinction between time and intervals; adding two TIME values
-- together makes no sense and must be rejected.
SELECT f1 + time '00:01' AS `Illegal` FROM TIME_TBL;

DROP TABLE TIME_TBL;
