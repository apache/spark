--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- INT4
-- https://github.com/postgres/postgres/blob/REL_12_BETA1/src/test/regress/sql/int4.sql
--

CREATE TABLE INT4_TBL(f1 int) USING parquet;

-- [SPARK-28023] Trim the string when cast string type to other types
INSERT INTO INT4_TBL VALUES (trim('   0  '));

INSERT INTO INT4_TBL VALUES (trim('123456     '));

INSERT INTO INT4_TBL VALUES (trim('    -123456'));

-- [SPARK-27923] Invalid input syntax for integer: "34.5" at PostgreSQL
-- INSERT INTO INT4_TBL(f1) VALUES ('34.5');

-- largest and smallest values
INSERT INTO INT4_TBL VALUES ('2147483647');

INSERT INTO INT4_TBL VALUES ('-2147483647');

-- [SPARK-27923] Spark SQL insert these bad inputs to NULL
-- bad input values
-- INSERT INTO INT4_TBL(f1) VALUES ('1000000000000');
-- INSERT INTO INT4_TBL(f1) VALUES ('asdf');
-- INSERT INTO INT4_TBL(f1) VALUES ('     ');
-- INSERT INTO INT4_TBL(f1) VALUES ('   asdf   ');
-- INSERT INTO INT4_TBL(f1) VALUES ('- 1234');
-- INSERT INTO INT4_TBL(f1) VALUES ('123       5');
-- INSERT INTO INT4_TBL(f1) VALUES ('');


SELECT '' AS five, * FROM INT4_TBL;

SELECT '' AS four, i.* FROM INT4_TBL i WHERE i.f1 <> smallint('0');

SELECT '' AS four, i.* FROM INT4_TBL i WHERE i.f1 <> int('0');

SELECT '' AS one, i.* FROM INT4_TBL i WHERE i.f1 = smallint('0');

SELECT '' AS one, i.* FROM INT4_TBL i WHERE i.f1 = int('0');

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 < smallint('0');

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 < int('0');

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 <= smallint('0');

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 <= int('0');

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 > smallint('0');

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 > int('0');

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 >= smallint('0');

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 >= int('0');

-- positive odds
SELECT '' AS one, i.* FROM INT4_TBL i WHERE (i.f1 % smallint('2')) = smallint('1');

-- any evens
SELECT '' AS three, i.* FROM INT4_TBL i WHERE (i.f1 % int('2')) = smallint('0');

-- [SPARK-28024] Incorrect value when out of range
SELECT '' AS five, i.f1, i.f1 * smallint('2') AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 * smallint('2') AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

-- [SPARK-28024] Incorrect value when out of range
SELECT '' AS five, i.f1, i.f1 * int('2') AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 * int('2') AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

-- [SPARK-28024] Incorrect value when out of range
SELECT '' AS five, i.f1, i.f1 + smallint('2') AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 + smallint('2') AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

-- [SPARK-28024] Incorrect value when out of range
SELECT '' AS five, i.f1, i.f1 + int('2') AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 + int('2') AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

-- [SPARK-28024] Incorrect value when out of range
SELECT '' AS five, i.f1, i.f1 - smallint('2') AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 - smallint('2') AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

-- [SPARK-28024] Incorrect value when out of range
SELECT '' AS five, i.f1, i.f1 - int('2') AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 - int('2') AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

SELECT '' AS five, i.f1, i.f1 / smallint('2') AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 / int('2') AS x FROM INT4_TBL i;

--
-- more complex expressions
--

-- variations on unary minus parsing
SELECT -2+3 AS one;

SELECT 4-2 AS two;

SELECT 2- -1 AS three;

SELECT 2 - -2 AS four;

SELECT smallint('2') * smallint('2') = smallint('16') / smallint('4') AS true;

SELECT int('2') * smallint('2') = smallint('16') / int('4') AS true;

SELECT smallint('2') * int('2') = int('16') / smallint('4') AS true;

SELECT int('1000') < int('999') AS false;

-- [SPARK-28027] Our ! and !! has different meanings
-- SELECT 4! AS twenty_four;

-- SELECT !!3 AS six;

SELECT 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS ten;

-- [SPARK-2659] HiveQL: Division operator should always perform fractional division
SELECT 2 + 2 / 2 AS three;

SELECT (2 + 2) / 2 AS two;

-- [SPARK-28027] Add bitwise shift left/right operators
-- corner case
SELECT string(shiftleft(int(-1), 31));
SELECT string(int(shiftleft(int(-1), 31))+1);

-- [SPARK-28024] Incorrect numeric values when out of range
-- check sane handling of INT_MIN overflow cases
-- SELECT (-2147483648)::int4 * (-1)::int4;
-- SELECT (-2147483648)::int4 / (-1)::int4;
SELECT int(-2147483648) % int(-1);
-- SELECT (-2147483648)::int4 * (-1)::int2;
-- SELECT (-2147483648)::int4 / (-1)::int2;
SELECT int(-2147483648) % smallint(-1);

-- [SPARK-28028] Cast numeric to integral type need round
-- check rounding when casting from float
SELECT x, int(x) AS int4_value
FROM (VALUES double(-2.5),
             double(-1.5),
             double(-0.5),
             double(0.0),
             double(0.5),
             double(1.5),
             double(2.5)) t(x);

-- [SPARK-28028] Cast numeric to integral type need round
-- check rounding when casting from numeric
SELECT x, int(x) AS int4_value
FROM (VALUES cast(-2.5 as decimal(38, 18)),
             cast(-1.5 as decimal(38, 18)),
             cast(-0.5 as decimal(38, 18)),
             cast(-0.0 as decimal(38, 18)),
             cast(0.5 as decimal(38, 18)),
             cast(1.5 as decimal(38, 18)),
             cast(2.5 as decimal(38, 18))) t(x);

DROP TABLE INT4_TBL;
