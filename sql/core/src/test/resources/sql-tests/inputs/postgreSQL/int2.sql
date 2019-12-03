--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- INT2
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/int2.sql

CREATE TABLE INT2_TBL(f1 smallint) USING parquet;

-- [SPARK-28023] Trim the string when cast string type to other types
-- PostgreSQL implicitly casts string literals to data with integral types, but
-- Spark does not support that kind of implicit casts.
INSERT INTO INT2_TBL VALUES (smallint(trim('0   ')));

INSERT INTO INT2_TBL VALUES (smallint(trim('  1234 ')));

INSERT INTO INT2_TBL VALUES (smallint(trim('    -1234')));

-- [SPARK-27923] Invalid input syntax for type short throws exception at PostgreSQL
-- INSERT INTO INT2_TBL VALUES ('34.5');

-- largest and smallest values
-- PostgreSQL implicitly casts string literals to data with integral types, but
-- Spark does not support that kind of implicit casts.
INSERT INTO INT2_TBL VALUES (smallint('32767'));

INSERT INTO INT2_TBL VALUES (smallint('-32767'));

-- bad input values -- should give errors
-- INSERT INTO INT2_TBL VALUES ('100000');
-- INSERT INTO INT2_TBL VALUES ('asdf');
-- INSERT INTO INT2_TBL VALUES ('    ');
-- INSERT INTO INT2_TBL VALUES ('- 1234');
-- INSERT INTO INT2_TBL VALUES ('4 444');
-- INSERT INTO INT2_TBL VALUES ('123 dt');
-- INSERT INTO INT2_TBL VALUES ('');


SELECT '' AS five, * FROM INT2_TBL;

SELECT '' AS four, i.* FROM INT2_TBL i WHERE i.f1 <> smallint('0');

SELECT '' AS four, i.* FROM INT2_TBL i WHERE i.f1 <> int('0');

SELECT '' AS one, i.* FROM INT2_TBL i WHERE i.f1 = smallint('0');

SELECT '' AS one, i.* FROM INT2_TBL i WHERE i.f1 = int('0');

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 < smallint('0');

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 < int('0');

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 <= smallint('0');

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 <= int('0');

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 > smallint('0');

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 > int('0');

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 >= smallint('0');

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 >= int('0');

-- positive odds
SELECT '' AS one, i.* FROM INT2_TBL i WHERE (i.f1 % smallint('2')) = smallint('1');

-- any evens
SELECT '' AS three, i.* FROM INT2_TBL i WHERE (i.f1 % int('2')) = smallint('0');

-- [SPARK-28024] Incorrect value when out of range
-- SELECT '' AS five, i.f1, i.f1 * smallint('2') AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 * smallint('2') AS x FROM INT2_TBL i
WHERE abs(f1) < 16384;

SELECT '' AS five, i.f1, i.f1 * int('2') AS x FROM INT2_TBL i;

-- [SPARK-28024] Incorrect value when out of range
-- SELECT '' AS five, i.f1, i.f1 + smallint('2') AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 + smallint('2') AS x FROM INT2_TBL i
WHERE f1 < 32766;

SELECT '' AS five, i.f1, i.f1 + int('2') AS x FROM INT2_TBL i;

-- [SPARK-28024] Incorrect value when out of range
-- SELECT '' AS five, i.f1, i.f1 - smallint('2') AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 - smallint('2') AS x FROM INT2_TBL i
WHERE f1 > -32767;

SELECT '' AS five, i.f1, i.f1 - int('2') AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 / smallint('2') AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 / int('2') AS x FROM INT2_TBL i;

-- corner cases
SELECT string(shiftleft(smallint(-1), 15));
SELECT string(smallint(shiftleft(smallint(-1), 15))+1);

-- check sane handling of INT16_MIN overflow cases
-- [SPARK-28024] Incorrect numeric values when out of range
-- SELECT smallint((-32768)) * smallint(-1);
-- SELECT smallint(-32768) / smallint(-1);
SELECT smallint(-32768) % smallint(-1);

-- [SPARK-28028] Cast numeric to integral type need round
-- check rounding when casting from float
SELECT x, smallint(x) AS int2_value
FROM (VALUES float(-2.5),
             float(-1.5),
             float(-0.5),
             float(0.0),
             float(0.5),
             float(1.5),
             float(2.5)) t(x);

-- [SPARK-28028] Cast numeric to integral type need round
-- check rounding when casting from numeric
SELECT x, smallint(x) AS int2_value
FROM (VALUES cast(-2.5 as decimal(38, 18)),
             cast(-1.5 as decimal(38, 18)),
             cast(-0.5 as decimal(38, 18)),
             cast(-0.0 as decimal(38, 18)),
             cast(0.5 as decimal(38, 18)),
             cast(1.5 as decimal(38, 18)),
             cast(2.5 as decimal(38, 18))) t(x);

DROP TABLE INT2_TBL;
