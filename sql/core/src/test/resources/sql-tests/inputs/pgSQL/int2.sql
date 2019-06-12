--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- INT2
-- https://github.com/postgres/postgres/blob/REL_12_BETA1/src/test/regress/sql/int2.sql

CREATE TABLE INT2_TBL(f1 int2);

INSERT INTO INT2_TBL(f1) VALUES ('0   ');

INSERT INTO INT2_TBL(f1) VALUES ('  1234 ');

INSERT INTO INT2_TBL(f1) VALUES ('    -1234');

INSERT INTO INT2_TBL(f1) VALUES ('34.5');

-- largest and smallest values
INSERT INTO INT2_TBL(f1) VALUES ('32767');

INSERT INTO INT2_TBL(f1) VALUES ('-32767');

-- bad input values -- should give errors
INSERT INTO INT2_TBL(f1) VALUES ('100000');
INSERT INTO INT2_TBL(f1) VALUES ('asdf');
INSERT INTO INT2_TBL(f1) VALUES ('    ');
INSERT INTO INT2_TBL(f1) VALUES ('- 1234');
INSERT INTO INT2_TBL(f1) VALUES ('4 444');
INSERT INTO INT2_TBL(f1) VALUES ('123 dt');
INSERT INTO INT2_TBL(f1) VALUES ('');


SELECT '' AS five, * FROM INT2_TBL;

SELECT '' AS four, i.* FROM INT2_TBL i WHERE i.f1 <> int2 '0';

SELECT '' AS four, i.* FROM INT2_TBL i WHERE i.f1 <> int4 '0';

SELECT '' AS one, i.* FROM INT2_TBL i WHERE i.f1 = int2 '0';

SELECT '' AS one, i.* FROM INT2_TBL i WHERE i.f1 = int4 '0';

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 < int2 '0';

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 < int4 '0';

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 <= int2 '0';

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 <= int4 '0';

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 > int2 '0';

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 > int4 '0';

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 >= int2 '0';

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 >= int4 '0';

-- positive odds
SELECT '' AS one, i.* FROM INT2_TBL i WHERE (i.f1 % int2 '2') = int2 '1';

-- any evens
SELECT '' AS three, i.* FROM INT2_TBL i WHERE (i.f1 % int4 '2') = int2 '0';

SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT2_TBL i
WHERE abs(f1) < 16384;

SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT2_TBL i
WHERE f1 < 32766;

SELECT '' AS five, i.f1, i.f1 + int4 '2' AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT2_TBL i
WHERE f1 > -32767;

SELECT '' AS five, i.f1, i.f1 - int4 '2' AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 / int2 '2' AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 / int4 '2' AS x FROM INT2_TBL i;

-- corner cases
SELECT (-1::int2<<15)::text;
SELECT ((-1::int2<<15)+1::int2)::text;

-- check sane handling of INT16_MIN overflow cases
SELECT (-32768)::int2 * (-1)::int2;
SELECT (-32768)::int2 / (-1)::int2;
SELECT (-32768)::int2 % (-1)::int2;

-- check rounding when casting from float
SELECT x, x::int2 AS int2_value
FROM (VALUES (-2.5::float8),
             (-1.5::float8),
             (-0.5::float8),
             (0.0::float8),
             (0.5::float8),
             (1.5::float8),
             (2.5::float8)) t(x);

-- check rounding when casting from numeric
SELECT x, x::int2 AS int2_value
FROM (VALUES (-2.5::numeric),
             (-1.5::numeric),
             (-0.5::numeric),
             (0.0::numeric),
             (0.5::numeric),
             (1.5::numeric),
             (2.5::numeric)) t(x);
