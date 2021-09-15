
-- unary minus and plus
select -100;
select +230;
select -5.2;
select +6.8e0;
select -key, +key from testdata where key = 2;
select -(key + 1), - key + 1, +(key + 5) from testdata where key = 1;
select -max(key), +max(key) from testdata;
select - (-10);
select + (-key) from testdata where key = 32;
select - (+max(key)) from testdata;
select - - 3;
select - + 20;
select + + 100;
select - - max(key) from testdata;
select + - key from testdata where key = 33;

-- division
select 5 / 2;
select 5 / 0;
select 5 / null;
select null / 5;

-- integral div
select 5 div 2;
select 5 div 0;
select 5 div null;
select null div 5;
select cast(51 as decimal(10, 0)) div cast(2 as decimal(2, 0));
select cast(5 as decimal(1, 0)) div cast(0 as decimal(2, 0));
select cast(5 as decimal(1, 0)) div cast(null as decimal(2, 0));
select cast(null as decimal(1, 0)) div cast(5 as decimal(2, 0));

-- other arithmetics
select 1 + 2;
select 1 - 2;
select 2 * 5;
select 5 % 3;
select pmod(-7, 3);

-- math functions
select sec(1);
select sec(null);
select sec(0);
select sec(-1);
select csc(1);
select csc(null);
select csc(0);
select csc(-1);
select cot(1);
select cot(null);
select cot(0);
select cot(-1);

-- ceil and ceiling
select ceiling(0);
select ceiling(1);
select ceil(1234567890123456);
select ceiling(1234567890123456);
select ceil(0.01);
select ceiling(-0.10);

-- floor
select floor(0);
select floor(1);
select floor(1234567890123456);
select floor(0.01);
select floor(-0.10);

-- comparison operator
select 1 > 0.00001;

-- mod
select mod(7, 2), mod(7, 0), mod(0, 2), mod(7, null), mod(null, 2), mod(null, null);

-- length
select BIT_LENGTH('abc');
select CHAR_LENGTH('abc');
select CHARACTER_LENGTH('abc');
select OCTET_LENGTH('abc');

-- abs
select abs(-3.13), abs('-2.19');

-- positive/negative
select positive('-1.11'), positive(-1.11), negative('-1.11'), negative(-1.11);

-- pmod
select pmod(-7, 2), pmod(0, 2), pmod(7, 0), pmod(7, null), pmod(null, 2), pmod(null, null);
select pmod(cast(3.13 as decimal), cast(0 as decimal)), pmod(cast(2 as smallint), cast(0 as smallint));

-- width_bucket
select width_bucket(5.35, 0.024, 10.06, 5);
select width_bucket(5.35, 0.024, 10.06, 3 + 2);
select width_bucket('5.35', '0.024', '10.06', '5');
select width_bucket(5.35, 0.024, 10.06, 2.5);
select width_bucket(5.35, 0.024, 10.06, 0.5);
select width_bucket(null, 0.024, 10.06, 5);
select width_bucket(5.35, null, 10.06, 5);
select width_bucket(5.35, 0.024, null, -5);
select width_bucket(5.35, 0.024, 10.06, null);
select width_bucket(5.35, 0.024, 10.06, -5);
select width_bucket(5.35, 0.024, 10.06, 9223372036854775807L); -- long max value
select width_bucket(5.35, 0.024, 10.06, 9223372036854775807L - 1);
