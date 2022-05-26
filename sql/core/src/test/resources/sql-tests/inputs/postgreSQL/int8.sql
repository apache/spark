--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- INT8
-- Test int8 64-bit integers.
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/int8.sql
--
CREATE TABLE INT8_TBL(q1 bigint, q2 bigint) USING parquet;

-- PostgreSQL implicitly casts string literals to data with integral types, but
-- Spark does not support that kind of implicit casts.
INSERT INTO INT8_TBL VALUES(bigint(trim('  123   ')),bigint(trim('  456')));
INSERT INTO INT8_TBL VALUES(bigint(trim('123   ')),bigint('4567890123456789'));
INSERT INTO INT8_TBL VALUES(bigint('4567890123456789'),bigint('123'));
INSERT INTO INT8_TBL VALUES(+4567890123456789,bigint('4567890123456789'));
INSERT INTO INT8_TBL VALUES(bigint('+4567890123456789'),bigint('-4567890123456789'));

-- [SPARK-27923] Spark SQL insert there bad inputs to NULL
-- bad inputs
-- INSERT INTO INT8_TBL(q1) VALUES ('      ');
-- INSERT INTO INT8_TBL(q1) VALUES ('xxx');
-- INSERT INTO INT8_TBL(q1) VALUES ('3908203590239580293850293850329485');
-- INSERT INTO INT8_TBL(q1) VALUES ('-1204982019841029840928340329840934');
-- INSERT INTO INT8_TBL(q1) VALUES ('- 123');
-- INSERT INTO INT8_TBL(q1) VALUES ('  345     5');
-- INSERT INTO INT8_TBL(q1) VALUES ('');

SELECT * FROM INT8_TBL;

-- int8/int8 cmp
SELECT * FROM INT8_TBL WHERE q2 = 4567890123456789;
SELECT * FROM INT8_TBL WHERE q2 <> 4567890123456789;
SELECT * FROM INT8_TBL WHERE q2 < 4567890123456789;
SELECT * FROM INT8_TBL WHERE q2 > 4567890123456789;
SELECT * FROM INT8_TBL WHERE q2 <= 4567890123456789;
SELECT * FROM INT8_TBL WHERE q2 >= 4567890123456789;

-- int8/int4 cmp
SELECT * FROM INT8_TBL WHERE q2 = 456;
SELECT * FROM INT8_TBL WHERE q2 <> 456;
SELECT * FROM INT8_TBL WHERE q2 < 456;
SELECT * FROM INT8_TBL WHERE q2 > 456;
SELECT * FROM INT8_TBL WHERE q2 <= 456;
SELECT * FROM INT8_TBL WHERE q2 >= 456;

-- int4/int8 cmp
SELECT * FROM INT8_TBL WHERE 123 = q1;
SELECT * FROM INT8_TBL WHERE 123 <> q1;
SELECT * FROM INT8_TBL WHERE 123 < q1;
SELECT * FROM INT8_TBL WHERE 123 > q1;
SELECT * FROM INT8_TBL WHERE 123 <= q1;
SELECT * FROM INT8_TBL WHERE 123 >= q1;

-- int8/int2 cmp
SELECT * FROM INT8_TBL WHERE q2 = smallint('456');
SELECT * FROM INT8_TBL WHERE q2 <> smallint('456');
SELECT * FROM INT8_TBL WHERE q2 < smallint('456');
SELECT * FROM INT8_TBL WHERE q2 > smallint('456');
SELECT * FROM INT8_TBL WHERE q2 <= smallint('456');
SELECT * FROM INT8_TBL WHERE q2 >= smallint('456');

-- int2/int8 cmp
SELECT * FROM INT8_TBL WHERE smallint('123') = q1;
SELECT * FROM INT8_TBL WHERE smallint('123') <> q1;
SELECT * FROM INT8_TBL WHERE smallint('123') < q1;
SELECT * FROM INT8_TBL WHERE smallint('123') > q1;
SELECT * FROM INT8_TBL WHERE smallint('123') <= q1;
SELECT * FROM INT8_TBL WHERE smallint('123') >= q1;


-- [SPARK-28349] We do not need to follow PostgreSQL to support reserved words in column alias
SELECT '' AS five, q1 AS plus, -q1 AS `minus` FROM INT8_TBL;

SELECT '' AS five, q1, q2, q1 + q2 AS plus FROM INT8_TBL;
SELECT '' AS five, q1, q2, q1 - q2 AS `minus` FROM INT8_TBL;
SELECT '' AS three, q1, q2, q1 * q2 AS multiply FROM INT8_TBL;
SELECT '' AS three, q1, q2, q1 * q2 AS multiply FROM INT8_TBL
 WHERE q1 < 1000 or (q2 > 0 and q2 < 1000);
SELECT '' AS five, q1, q2, q1 / q2 AS divide, q1 % q2 AS mod FROM INT8_TBL;

SELECT '' AS five, q1, double(q1) FROM INT8_TBL;
SELECT '' AS five, q2, double(q2) FROM INT8_TBL;

SELECT 37 + q1 AS plus4 FROM INT8_TBL;
SELECT 37 - q1 AS minus4 FROM INT8_TBL;
SELECT '' AS five, 2 * q1 AS `twice int4` FROM INT8_TBL;
SELECT '' AS five, q1 * 2 AS `twice int4` FROM INT8_TBL;

-- int8 op int4
SELECT q1 + int(42) AS `8plus4`, q1 - int(42) AS `8minus4`, q1 * int(42) AS `8mul4`, q1 / int(42) AS `8div4` FROM INT8_TBL;
-- int4 op int8
SELECT int(246) + q1 AS `4plus8`, int(246) - q1 AS `4minus8`, int(246) * q1 AS `4mul8`, int(246) / q1 AS `4div8` FROM INT8_TBL;

-- int8 op int2
SELECT q1 + smallint(42) AS `8plus2`, q1 - smallint(42) AS `8minus2`, q1 * smallint(42) AS `8mul2`, q1 / smallint(42) AS `8div2` FROM INT8_TBL;
-- int2 op int8
SELECT smallint(246) + q1 AS `2plus8`, smallint(246) - q1 AS `2minus8`, smallint(246) * q1 AS `2mul8`, smallint(246) / q1 AS `2div8` FROM INT8_TBL;

SELECT q2, abs(q2) FROM INT8_TBL;
SELECT min(q1), min(q2) FROM INT8_TBL;
SELECT max(q1), max(q2) FROM INT8_TBL;

-- TO_CHAR()
-- some queries are commented out as the format string is not supported by Spark
SELECT '' AS to_char_1, to_char(q1, '9G999G999G999G999G999'), to_char(q2, '9,999,999,999,999,999')
FROM INT8_TBL;

-- SELECT '' AS to_char_2, to_char(q1, '9G999G999G999G999G999D999G999'), to_char(q2, '9,999,999,999,999,999.999,999')
-- FROM INT8_TBL;

SELECT '' AS to_char_3, to_char( (q1 * -1), '9999999999999999PR'), to_char( (q2 * -1), '9999999999999999.999PR')
FROM INT8_TBL;

SELECT '' AS to_char_4, to_char( (q1 * -1), '9999999999999999S'), to_char( (q2 * -1), 'S9999999999999999')
FROM INT8_TBL;

SELECT '' AS to_char_5,  to_char(q2, 'MI9999999999999999')     FROM INT8_TBL;
-- SELECT '' AS to_char_6,  to_char(q2, 'FMS9999999999999999')    FROM INT8_TBL;
-- SELECT '' AS to_char_7,  to_char(q2, 'FM9999999999999999THPR') FROM INT8_TBL;
-- SELECT '' AS to_char_8,  to_char(q2, 'SG9999999999999999th')   FROM INT8_TBL;
SELECT '' AS to_char_9,  to_char(q2, '0999999999999999')       FROM INT8_TBL;
SELECT '' AS to_char_10, to_char(q2, 'S0999999999999999')      FROM INT8_TBL;
-- SELECT '' AS to_char_11, to_char(q2, 'FM0999999999999999')     FROM INT8_TBL;
-- SELECT '' AS to_char_12, to_char(q2, 'FM9999999999999999.000') FROM INT8_TBL;
-- SELECT '' AS to_char_13, to_char(q2, 'L9999999999999999.000')  FROM INT8_TBL;
-- SELECT '' AS to_char_14, to_char(q2, 'FM9999999999999999.999') FROM INT8_TBL;
-- SELECT '' AS to_char_15, to_char(q2, 'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9') FROM INT8_TBL;
-- SELECT '' AS to_char_16, to_char(q2, E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999') FROM INT8_TBL;
-- SELECT '' AS to_char_17, to_char(q2, '999999SG9999999999')     FROM INT8_TBL;

-- [SPARK-26218] Throw exception on overflow for integers
-- check min/max values and overflow behavior

-- select bigint('-9223372036854775808');
-- select bigint('-9223372036854775809');
-- select bigint('9223372036854775807');
-- select bigint('9223372036854775808');

-- select bigint('9223372036854775808');

-- select -(bigint('-9223372036854775807'));
-- select -(bigint('-9223372036854775808'));

-- select bigint('9223372036854775800') + bigint('9223372036854775800');
-- select bigint('-9223372036854775800') + bigint('-9223372036854775800');

-- select bigint('9223372036854775800') - bigint('-9223372036854775800');
-- select bigint('-9223372036854775800') - bigint('9223372036854775800');

-- select bigint('9223372036854775800') * bigint('9223372036854775800');

select bigint('9223372036854775800') / bigint('0');
-- select bigint('9223372036854775800') % bigint('0');

-- select abs(bigint('-9223372036854775808'));

-- select bigint('9223372036854775800') + int('100');
-- select bigint('-9223372036854775800') - int('100');
-- select bigint('9223372036854775800') * int('100');

-- select int('100') + bigint('9223372036854775800');
-- select int('-100') - bigint('9223372036854775800');
-- select int('100') * bigint('9223372036854775800');

-- select bigint('9223372036854775800') + smallint('100');
-- select bigint('-9223372036854775800') - smallint('100');
-- select bigint('9223372036854775800') * smallint('100');
select bigint('-9223372036854775808') / smallint('0');

-- select smallint('100') + bigint('9223372036854775800');
-- select smallint('-100') - bigint('9223372036854775800');
-- select smallint('100') * bigint('9223372036854775800');
select smallint('100') / bigint('0');

SELECT CAST(q1 AS int) FROM int8_tbl WHERE q2 = 456;
SELECT CAST(q1 AS int) FROM int8_tbl WHERE q2 <> 456;

SELECT CAST(q1 AS smallint) FROM int8_tbl WHERE q2 = 456;
SELECT CAST(q1 AS smallint) FROM int8_tbl WHERE q2 <> 456;

SELECT CAST(smallint('42') AS bigint), CAST(smallint('-37') AS bigint);

SELECT CAST(q1 AS float), CAST(q2 AS double) FROM INT8_TBL;
SELECT CAST(float('36854775807.0') AS bigint);
SELECT CAST(double('922337203685477580700.0') AS bigint);


-- [SPARK-28027] Missing some mathematical operators
-- bit operations

-- SELECT q1, q2, q1 & q2 AS `and`, q1 | q2 AS `or`, q1 # q2 AS `xor`, ~q1 AS `not` FROM INT8_TBL;
SELECT q1, q2, q1 & q2 AS `and`, q1 | q2 AS `or`, ~q1 AS `not` FROM INT8_TBL;
-- SELECT q1, q1 << 2 AS `shl`, q1 >> 3 AS `shr` FROM INT8_TBL;


-- generate_series

SELECT * FROM range(bigint('+4567890123456789'), bigint('+4567890123456799'));
SELECT * FROM range(bigint('+4567890123456789'), bigint('+4567890123456799'), 0);
SELECT * FROM range(bigint('+4567890123456789'), bigint('+4567890123456799'), 2);

-- corner case
SELECT string(shiftleft(bigint(-1), 63));
SELECT string(int(shiftleft(bigint(-1), 63))+1);

-- [SPARK-26218] Throw exception on overflow for integers
-- check sane handling of INT64_MIN overflow cases
SELECT bigint((-9223372036854775808)) * bigint((-1));
SELECT bigint((-9223372036854775808)) / bigint((-1));
SELECT bigint((-9223372036854775808)) % bigint((-1));
SELECT bigint((-9223372036854775808)) * int((-1));
SELECT bigint((-9223372036854775808)) / int((-1));
SELECT bigint((-9223372036854775808)) % int((-1));
SELECT bigint((-9223372036854775808)) * smallint((-1));
SELECT bigint((-9223372036854775808)) / smallint((-1));
SELECT bigint((-9223372036854775808)) % smallint((-1));

-- [SPARK-28028] Cast numeric to integral type need round
-- check rounding when casting from float
SELECT x, bigint(x) AS int8_value
FROM (VALUES (double(-2.5)),
             (double(-1.5)),
             (double(-0.5)),
             (double(0.0)),
             (double(0.5)),
             (double(1.5)),
             (double(2.5))) t(x);

-- check rounding when casting from numeric
SELECT x, bigint(x) AS int8_value
FROM (VALUES cast(-2.5 as decimal(38, 18)),
             cast(-1.5 as decimal(38, 18)),
             cast(-0.5 as decimal(38, 18)),
             cast(-0.0 as decimal(38, 18)),
             cast(0.5 as decimal(38, 18)),
             cast(1.5 as decimal(38, 18)),
             cast(2.5 as decimal(38, 18))) t(x);

DROP TABLE INT8_TBL;
