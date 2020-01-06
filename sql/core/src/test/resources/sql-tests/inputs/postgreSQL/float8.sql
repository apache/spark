--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- FLOAT8
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/float8.sql

CREATE TABLE FLOAT8_TBL(f1 double) USING parquet;

-- PostgreSQL implicitly casts string literals to data with floating point types, but
-- Spark does not support that kind of implicit casts.
INSERT INTO FLOAT8_TBL VALUES (double('    0.0   '));
INSERT INTO FLOAT8_TBL VALUES (double('1004.30  '));
INSERT INTO FLOAT8_TBL VALUES (double('   -34.84'));
INSERT INTO FLOAT8_TBL VALUES (double('1.2345678901234e+200'));
INSERT INTO FLOAT8_TBL VALUES (double('1.2345678901234e-200'));

-- [SPARK-28024] Incorrect numeric values when out of range
-- test for underflow and overflow handling
SELECT double('10e400');
SELECT double('-10e400');
SELECT double('10e-400');
SELECT double('-10e-400');

-- [SPARK-28061] Support for converting float to binary format
-- test smallest normalized input
-- SELECT float8send('2.2250738585072014E-308'::float8);

-- [SPARK-27923] Spark SQL insert there bad inputs to NULL
-- bad input
-- INSERT INTO FLOAT8_TBL VALUES ('');
-- INSERT INTO FLOAT8_TBL VALUES ('     ');
-- INSERT INTO FLOAT8_TBL VALUES ('xyz');
-- INSERT INTO FLOAT8_TBL VALUES ('5.0.0');
-- INSERT INTO FLOAT8_TBL VALUES ('5 . 0');
-- INSERT INTO FLOAT8_TBL VALUES ('5.   0');
-- INSERT INTO FLOAT8_TBL VALUES ('    - 3');
-- INSERT INTO FLOAT8_TBL VALUES ('123           5');

-- special inputs
SELECT double('NaN');
SELECT double('nan');
SELECT double('   NAN  ');
SELECT double('infinity');
SELECT double('          -INFINiTY   ');
-- [SPARK-27923] Spark SQL insert there bad special inputs to NULL
-- bad special inputs
SELECT double('N A N');
SELECT double('NaN x');
SELECT double(' INFINITY    x');

SELECT double('Infinity') + 100.0;
SELECT double('Infinity') / double('Infinity');
SELECT double('NaN') / double('NaN');
-- [SPARK-28315] Decimal can not accept NaN as input
SELECT double(decimal('nan'));

SELECT '' AS five, * FROM FLOAT8_TBL;

SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE f.f1 <> '1004.3';

SELECT '' AS one, f.* FROM FLOAT8_TBL f WHERE f.f1 = '1004.3';

SELECT '' AS three, f.* FROM FLOAT8_TBL f WHERE '1004.3' > f.f1;

SELECT '' AS three, f.* FROM FLOAT8_TBL f WHERE  f.f1 < '1004.3';

SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE '1004.3' >= f.f1;

SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE  f.f1 <= '1004.3';

SELECT '' AS three, f.f1, f.f1 * '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 + '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 / '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 - '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';
-- [SPARK-28007] Caret operator (^) means bitwise XOR in Spark/Hive and exponentiation in Postgres
-- SELECT '' AS one, f.f1 ^ '2.0' AS square_f1
--    FROM FLOAT8_TBL f where f.f1 = '1004.3';

-- [SPARK-28027] Spark SQL does not support prefix operator @
-- absolute value
-- SELECT '' AS five, f.f1, @f.f1 AS abs_f1
--    FROM FLOAT8_TBL f;

-- [SPARK-23906] Support Truncate number
-- truncate
-- SELECT '' AS five, f.f1, trunc(f.f1) AS trunc_f1
--    FROM FLOAT8_TBL f;

-- round
SELECT '' AS five, f.f1, round(f.f1) AS round_f1
   FROM FLOAT8_TBL f;

-- [SPARK-28135] ceil/ceiling/floor returns incorrect values
-- ceil / ceiling
select ceil(f1) as ceil_f1 from float8_tbl f;
select ceiling(f1) as ceiling_f1 from float8_tbl f;

-- floor
select floor(f1) as floor_f1 from float8_tbl f;

-- sign
select sign(f1) as sign_f1 from float8_tbl f;

-- avoid bit-exact output here because operations may not be bit-exact.
-- SET extra_float_digits = 0;

-- square root
SELECT sqrt(double('64')) AS eight;

-- [SPARK-28027] Spark SQL does not support prefix operator |/
-- SELECT |/ float8 '64' AS eight;

-- SELECT '' AS three, f.f1, |/f.f1 AS sqrt_f1
--    FROM FLOAT8_TBL f
--    WHERE f.f1 > '0.0';

-- power
SELECT power(double('144'), double('0.5'));
SELECT power(double('NaN'), double('0.5'));
SELECT power(double('144'), double('NaN'));
SELECT power(double('NaN'), double('NaN'));
SELECT power(double('-1'), double('NaN'));
-- [SPARK-28135] power returns incorrect values
SELECT power(double('1'), double('NaN'));
SELECT power(double('NaN'), double('0'));

-- take exp of ln(f.f1)
SELECT '' AS three, f.f1, exp(ln(f.f1)) AS exp_ln_f1
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

-- [SPARK-28027] Spark SQL does not support prefix operator ||/
-- cube root
-- SELECT ||/ float8 '27' AS three;

-- SELECT '' AS five, f.f1, ||/f.f1 AS cbrt_f1 FROM FLOAT8_TBL f;


SELECT '' AS five, * FROM FLOAT8_TBL;

-- UPDATE FLOAT8_TBL
--    SET f1 = FLOAT8_TBL.f1 * '-1'
--    WHERE FLOAT8_TBL.f1 > '0.0';
-- Update the FLOAT8_TBL to UPDATED_FLOAT8_TBL
CREATE TEMPORARY VIEW UPDATED_FLOAT8_TBL as
SELECT
  CASE WHEN FLOAT8_TBL.f1 > '0.0' THEN FLOAT8_TBL.f1 * '-1' ELSE FLOAT8_TBL.f1 END AS f1
FROM FLOAT8_TBL;

-- [SPARK-27923] Out of range, Spark SQL returns Infinity
SELECT '' AS bad, f.f1 * '1e200' from UPDATED_FLOAT8_TBL f;

-- [SPARK-28007] Caret operator (^) means bitwise XOR in Spark/Hive and exponentiation in Postgres
-- SELECT '' AS bad, f.f1 ^ '1e200' from UPDATED_FLOAT8_TBL f;

-- SELECT 0 ^ 0 + 0 ^ 1 + 0 ^ 0.0 + 0 ^ 0.5;

-- [SPARK-27923] Cannot take logarithm of zero
-- SELECT '' AS bad, ln(f.f1) from UPDATED_FLOAT8_TBL f where f.f1 = '0.0' ;

-- [SPARK-27923] Cannot take logarithm of a negative number
-- SELECT '' AS bad, ln(f.f1) from UPDATED_FLOAT8_TBL f where f.f1 < '0.0' ;

-- [SPARK-28024] Incorrect numeric values when out of range
-- SELECT '' AS bad, exp(f.f1) from UPDATED_FLOAT8_TBL f;

-- [SPARK-27923] Divide by zero, Spark SQL returns NULL
-- SELECT '' AS bad, f.f1 / '0.0' from UPDATED_FLOAT8_TBL f;

SELECT '' AS five, * FROM UPDATED_FLOAT8_TBL;

-- hyperbolic functions
-- we run these with extra_float_digits = 0 too, since different platforms
-- tend to produce results that vary in the last place.
SELECT sinh(double('1'));
SELECT cosh(double('1'));
SELECT tanh(double('1'));
SELECT asinh(double('1'));
SELECT acosh(double('2'));
SELECT atanh(double('0.5'));

-- test Inf/NaN cases for hyperbolic functions
SELECT sinh(double('Infinity'));
SELECT sinh(double('-Infinity'));
SELECT sinh(double('NaN'));
SELECT cosh(double('Infinity'));
SELECT cosh(double('-Infinity'));
SELECT cosh(double('NaN'));
SELECT tanh(double('Infinity'));
SELECT tanh(double('-Infinity'));
SELECT tanh(double('NaN'));
SELECT asinh(double('Infinity'));
SELECT asinh(double('-Infinity'));
SELECT asinh(double('NaN'));
-- acosh(Inf) should be Inf, but some mingw versions produce NaN, so skip test
SELECT acosh(double('Infinity'));
SELECT acosh(double('-Infinity'));
SELECT acosh(double('NaN'));
SELECT atanh(double('Infinity'));
SELECT atanh(double('-Infinity'));
SELECT atanh(double('NaN'));

-- RESET extra_float_digits;

-- [SPARK-28024] Incorrect numeric values when out of range
-- test for over- and underflow
-- INSERT INTO FLOAT8_TBL VALUES ('10e400');

-- INSERT INTO FLOAT8_TBL VALUES ('-10e400');

-- INSERT INTO FLOAT8_TBL VALUES ('10e-400');

-- INSERT INTO FLOAT8_TBL VALUES ('-10e-400');

-- maintain external table consistency across platforms
-- delete all values and reinsert well-behaved ones

TRUNCATE TABLE FLOAT8_TBL;

-- PostgreSQL implicitly casts string literals to data with floating point types, but
-- Spark does not support that kind of implicit casts.
INSERT INTO FLOAT8_TBL VALUES (double('0.0'));

INSERT INTO FLOAT8_TBL VALUES (double('-34.84'));

INSERT INTO FLOAT8_TBL VALUES (double('-1004.30'));

INSERT INTO FLOAT8_TBL VALUES (double('-1.2345678901234e+200'));

INSERT INTO FLOAT8_TBL VALUES (double('-1.2345678901234e-200'));

SELECT '' AS five, * FROM FLOAT8_TBL;

-- [SPARK-28028] Cast numeric to integral type need round
-- [SPARK-28024] Incorrect numeric values when out of range
-- test edge-case coercions to integer
SELECT smallint(double('32767.4'));
SELECT smallint(double('32767.6'));
SELECT smallint(double('-32768.4'));
SELECT smallint(double('-32768.6'));
SELECT int(double('2147483647.4'));
SELECT int(double('2147483647.6'));
SELECT int(double('-2147483648.4'));
SELECT int(double('-2147483648.6'));
SELECT bigint(double('9223372036854773760'));
SELECT bigint(double('9223372036854775807'));
SELECT bigint(double('-9223372036854775808.5'));
SELECT bigint(double('-9223372036854780000'));

-- [SPARK-28134] Missing Trigonometric Functions
-- test exact cases for trigonometric functions in degrees

-- SELECT x,
--        sind(x),
--        sind(x) IN (-1,-0.5,0,0.5,1) AS sind_exact
-- FROM (VALUES (0), (30), (90), (150), (180),
--       (210), (270), (330), (360)) AS t(x);

-- SELECT x,
--        cosd(x),
--        cosd(x) IN (-1,-0.5,0,0.5,1) AS cosd_exact
-- FROM (VALUES (0), (60), (90), (120), (180),
--       (240), (270), (300), (360)) AS t(x);

-- SELECT x,
--        tand(x),
--        tand(x) IN ('-Infinity'::float8,-1,0,
--                    1,'Infinity'::float8) AS tand_exact,
--        cotd(x),
--        cotd(x) IN ('-Infinity'::float8,-1,0,
--                    1,'Infinity'::float8) AS cotd_exact
-- FROM (VALUES (0), (45), (90), (135), (180),
--       (225), (270), (315), (360)) AS t(x);

-- SELECT x,
--        asind(x),
--        asind(x) IN (-90,-30,0,30,90) AS asind_exact,
--        acosd(x),
--        acosd(x) IN (0,60,90,120,180) AS acosd_exact
-- FROM (VALUES (-1), (-0.5), (0), (0.5), (1)) AS t(x);

-- SELECT x,
--        atand(x),
--        atand(x) IN (-90,-45,0,45,90) AS atand_exact
-- FROM (VALUES ('-Infinity'::float8), (-1), (0), (1),
--       ('Infinity'::float8)) AS t(x);

-- SELECT x, y,
--        atan2d(y, x),
--        atan2d(y, x) IN (-90,0,90,180) AS atan2d_exact
-- FROM (SELECT 10*cosd(a), 10*sind(a)
--       FROM generate_series(0, 360, 90) AS t(a)) AS t(x,y);

-- We do not support creating types, skip the test below
--
-- test output (and round-trip safety) of various values.
-- To ensure we're testing what we think we're testing, start with
-- float values specified by bit patterns (as a useful side effect,
-- this means we'll fail on non-IEEE platforms).

-- create type xfloat8;
-- create function xfloat8in(cstring) returns xfloat8 immutable strict
--   language internal as 'int8in';
-- create function xfloat8out(xfloat8) returns cstring immutable strict
--   language internal as 'int8out';
-- create type xfloat8 (input = xfloat8in, output = xfloat8out, like = float8);
-- create cast (xfloat8 as float8) without function;
-- create cast (float8 as xfloat8) without function;
-- create cast (xfloat8 as bigint) without function;
-- create cast (bigint as xfloat8) without function;

-- float8: seeeeeee eeeeeeee eeeeeeee mmmmmmmm mmmmmmmm(x4)

-- we don't care to assume the platform's strtod() handles subnormals
-- correctly; those are "use at your own risk". However we do test
-- subnormal outputs, since those are under our control.

-- with testdata(bits) as (values
--   -- small subnormals
--   (x'0000000000000001'),
--   (x'0000000000000002'), (x'0000000000000003'),
--   (x'0000000000001000'), (x'0000000100000000'),
--   (x'0000010000000000'), (x'0000010100000000'),
--   (x'0000400000000000'), (x'0000400100000000'),
--   (x'0000800000000000'), (x'0000800000000001'),
--   -- these values taken from upstream testsuite
--   (x'00000000000f4240'),
--   (x'00000000016e3600'),
--   (x'0000008cdcdea440'),
--   -- borderline between subnormal and normal
--   (x'000ffffffffffff0'), (x'000ffffffffffff1'),
--   (x'000ffffffffffffe'), (x'000fffffffffffff'))
-- select float8send(flt) as ibits,
--        flt
--   from (select bits::bigint::xfloat8::float8 as flt
--           from testdata
-- 	offset 0) s;

-- round-trip tests

-- with testdata(bits) as (values
--   (x'0000000000000000'),
--   -- smallest normal values
--   (x'0010000000000000'), (x'0010000000000001'),
--   (x'0010000000000002'), (x'0018000000000000'),
--   --
--   (x'3ddb7cdfd9d7bdba'), (x'3ddb7cdfd9d7bdbb'), (x'3ddb7cdfd9d7bdbc'),
--   (x'3e112e0be826d694'), (x'3e112e0be826d695'), (x'3e112e0be826d696'),
--   (x'3e45798ee2308c39'), (x'3e45798ee2308c3a'), (x'3e45798ee2308c3b'),
--   (x'3e7ad7f29abcaf47'), (x'3e7ad7f29abcaf48'), (x'3e7ad7f29abcaf49'),
--   (x'3eb0c6f7a0b5ed8c'), (x'3eb0c6f7a0b5ed8d'), (x'3eb0c6f7a0b5ed8e'),
--   (x'3ee4f8b588e368ef'), (x'3ee4f8b588e368f0'), (x'3ee4f8b588e368f1'),
--   (x'3f1a36e2eb1c432c'), (x'3f1a36e2eb1c432d'), (x'3f1a36e2eb1c432e'),
--   (x'3f50624dd2f1a9fb'), (x'3f50624dd2f1a9fc'), (x'3f50624dd2f1a9fd'),
--   (x'3f847ae147ae147a'), (x'3f847ae147ae147b'), (x'3f847ae147ae147c'),
--   (x'3fb9999999999999'), (x'3fb999999999999a'), (x'3fb999999999999b'),
--   -- values very close to 1
--   (x'3feffffffffffff0'), (x'3feffffffffffff1'), (x'3feffffffffffff2'),
--   (x'3feffffffffffff3'), (x'3feffffffffffff4'), (x'3feffffffffffff5'),
--   (x'3feffffffffffff6'), (x'3feffffffffffff7'), (x'3feffffffffffff8'),
--   (x'3feffffffffffff9'), (x'3feffffffffffffa'), (x'3feffffffffffffb'),
--   (x'3feffffffffffffc'), (x'3feffffffffffffd'), (x'3feffffffffffffe'),
--   (x'3fefffffffffffff'),
--   (x'3ff0000000000000'),
--   (x'3ff0000000000001'), (x'3ff0000000000002'), (x'3ff0000000000003'),
--   (x'3ff0000000000004'), (x'3ff0000000000005'), (x'3ff0000000000006'),
--   (x'3ff0000000000007'), (x'3ff0000000000008'), (x'3ff0000000000009'),
--   --
--   (x'3ff921fb54442d18'),
--   (x'4005bf0a8b14576a'),
--   (x'400921fb54442d18'),
--   --
--   (x'4023ffffffffffff'), (x'4024000000000000'), (x'4024000000000001'),
--   (x'4058ffffffffffff'), (x'4059000000000000'), (x'4059000000000001'),
--   (x'408f3fffffffffff'), (x'408f400000000000'), (x'408f400000000001'),
--   (x'40c387ffffffffff'), (x'40c3880000000000'), (x'40c3880000000001'),
--   (x'40f869ffffffffff'), (x'40f86a0000000000'), (x'40f86a0000000001'),
--   (x'412e847fffffffff'), (x'412e848000000000'), (x'412e848000000001'),
--   (x'416312cfffffffff'), (x'416312d000000000'), (x'416312d000000001'),
--   (x'4197d783ffffffff'), (x'4197d78400000000'), (x'4197d78400000001'),
--   (x'41cdcd64ffffffff'), (x'41cdcd6500000000'), (x'41cdcd6500000001'),
--   (x'4202a05f1fffffff'), (x'4202a05f20000000'), (x'4202a05f20000001'),
--   (x'42374876e7ffffff'), (x'42374876e8000000'), (x'42374876e8000001'),
--   (x'426d1a94a1ffffff'), (x'426d1a94a2000000'), (x'426d1a94a2000001'),
--   (x'42a2309ce53fffff'), (x'42a2309ce5400000'), (x'42a2309ce5400001'),
--   (x'42d6bcc41e8fffff'), (x'42d6bcc41e900000'), (x'42d6bcc41e900001'),
--   (x'430c6bf52633ffff'), (x'430c6bf526340000'), (x'430c6bf526340001'),
--   (x'4341c37937e07fff'), (x'4341c37937e08000'), (x'4341c37937e08001'),
--   (x'4376345785d89fff'), (x'4376345785d8a000'), (x'4376345785d8a001'),
--   (x'43abc16d674ec7ff'), (x'43abc16d674ec800'), (x'43abc16d674ec801'),
--   (x'43e158e460913cff'), (x'43e158e460913d00'), (x'43e158e460913d01'),
--   (x'4415af1d78b58c3f'), (x'4415af1d78b58c40'), (x'4415af1d78b58c41'),
--   (x'444b1ae4d6e2ef4f'), (x'444b1ae4d6e2ef50'), (x'444b1ae4d6e2ef51'),
--   (x'4480f0cf064dd591'), (x'4480f0cf064dd592'), (x'4480f0cf064dd593'),
--   (x'44b52d02c7e14af5'), (x'44b52d02c7e14af6'), (x'44b52d02c7e14af7'),
--   (x'44ea784379d99db3'), (x'44ea784379d99db4'), (x'44ea784379d99db5'),
--   (x'45208b2a2c280290'), (x'45208b2a2c280291'), (x'45208b2a2c280292'),
--   --
--   (x'7feffffffffffffe'), (x'7fefffffffffffff'),
--   -- round to even tests (+ve)
--   (x'4350000000000002'),
--   (x'4350000000002e06'),
--   (x'4352000000000003'),
--   (x'4352000000000004'),
--   (x'4358000000000003'),
--   (x'4358000000000004'),
--   (x'435f000000000020'),
--   -- round to even tests (-ve)
--   (x'c350000000000002'),
--   (x'c350000000002e06'),
--   (x'c352000000000003'),
--   (x'c352000000000004'),
--   (x'c358000000000003'),
--   (x'c358000000000004'),
--   (x'c35f000000000020'),
--   -- exercise fixed-point memmoves
--   (x'42dc12218377de66'),
--   (x'42a674e79c5fe51f'),
--   (x'4271f71fb04cb74c'),
--   (x'423cbe991a145879'),
--   (x'4206fee0e1a9e061'),
--   (x'41d26580b487e6b4'),
--   (x'419d6f34540ca453'),
--   (x'41678c29dcd6e9dc'),
--   (x'4132d687e3df217d'),
--   (x'40fe240c9fcb68c8'),
--   (x'40c81cd6e63c53d3'),
--   (x'40934a4584fd0fdc'),
--   (x'405edd3c07fb4c93'),
--   (x'4028b0fcd32f7076'),
--   (x'3ff3c0ca428c59f8'),
--   -- these cases come from the upstream's testsuite
--   -- LotsOfTrailingZeros)
--   (x'3e60000000000000'),
--   -- Regression
--   (x'c352bd2668e077c4'),
--   (x'434018601510c000'),
--   (x'43d055dc36f24000'),
--   (x'43e052961c6f8000'),
--   (x'3ff3c0ca2a5b1d5d'),
--   -- LooksLikePow5
--   (x'4830f0cf064dd592'),
--   (x'4840f0cf064dd592'),
--   (x'4850f0cf064dd592'),
--   -- OutputLength
--   (x'3ff3333333333333'),
--   (x'3ff3ae147ae147ae'),
--   (x'3ff3be76c8b43958'),
--   (x'3ff3c083126e978d'),
--   (x'3ff3c0c1fc8f3238'),
--   (x'3ff3c0c9539b8887'),
--   (x'3ff3c0ca2a5b1d5d'),
--   (x'3ff3c0ca4283de1b'),
--   (x'3ff3c0ca43db770a'),
--   (x'3ff3c0ca428abd53'),
--   (x'3ff3c0ca428c1d2b'),
--   (x'3ff3c0ca428c51f2'),
--   (x'3ff3c0ca428c58fc'),
--   (x'3ff3c0ca428c59dd'),
--   (x'3ff3c0ca428c59f8'),
--   (x'3ff3c0ca428c59fb'),
--   -- 32-bit chunking
--   (x'40112e0be8047a7d'),
--   (x'40112e0be815a889'),
--   (x'40112e0be826d695'),
--   (x'40112e0be83804a1'),
--   (x'40112e0be84932ad'),
--   -- MinMaxShift
--   (x'0040000000000000'),
--   (x'007fffffffffffff'),
--   (x'0290000000000000'),
--   (x'029fffffffffffff'),
--   (x'4350000000000000'),
--   (x'435fffffffffffff'),
--   (x'1330000000000000'),
--   (x'133fffffffffffff'),
--   (x'3a6fa7161a4d6e0c')
-- )
-- select float8send(flt) as ibits,
--        flt,
--        flt::text::float8 as r_flt,
--        float8send(flt::text::float8) as obits,
--        float8send(flt::text::float8) = float8send(flt) as correct
--   from (select bits::bigint::xfloat8::float8 as flt
--           from testdata
-- 	offset 0) s;

-- clean up, lest opr_sanity complain
-- drop type xfloat8 cascade;
DROP TABLE FLOAT8_TBL;
