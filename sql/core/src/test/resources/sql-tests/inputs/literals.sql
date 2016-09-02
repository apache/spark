-- Literal parsing

-- null
select null, Null, nUll;

-- boolean
select true, tRue, false, fALse;

-- byte (tinyint)
select 1Y;
select 127Y, -128Y;

-- out of range byte
select 128Y;

-- short (smallint)
select 1S;
select 32767S, -32768S;

-- out of range short
select 32768S;

-- long (bigint)
select 1L, 2147483648L;
select 9223372036854775807L, -9223372036854775808L;

-- out of range long
select 9223372036854775808L;

-- integral parsing

-- parse int
select 1, -1;

-- parse int max and min value as int
select 2147483647, -2147483648;

-- parse long max and min value as long
select 9223372036854775807, -9223372036854775808;

-- parse as decimals (Long.MaxValue + 1, and Long.MinValue - 1)
select 9223372036854775808, -9223372036854775809;

-- out of range decimal numbers
select 1234567890123456789012345678901234567890;
select 1234567890123456789012345678901234567890.0;

-- double
select 1D, 1.2D, 1e10, 1.5e5, .10D, 0.10D, .1e5, .9e+2, 0.9e+2, 900e-1, 9.e+1;
select -1D, -1.2D, -1e10, -1.5e5, -.10D, -0.10D, -.1e5;
-- negative double
select .e3;
-- very large decimals (overflowing double).
select 1E309, -1E309;

-- decimal parsing
select 0.3, -0.8, .5, -.18, 0.1111, .1111;

-- super large scientific notation numbers should still be valid doubles
select 123456789012345678901234567890123456789e10d, 123456789012345678901234567890123456789.1e10d;

-- string
select "Hello Peter!", 'hello lee!';
-- multi string
select 'hello' 'world', 'hello' " " 'lee';
-- single quote within double quotes
select "hello 'peter'";
select 'pattern%', 'no-pattern\%', 'pattern\\%', 'pattern\\\%';
select '\'', '"', '\n', '\r', '\t', 'Z';
-- "Hello!" in octals
select '\110\145\154\154\157\041';
-- "World :)" in unicode
select '\u0057\u006F\u0072\u006C\u0064\u0020\u003A\u0029';

-- date
select dAte '2016-03-12';
-- invalid date
select date 'mar 11 2016';

-- timestamp
select tImEstAmp '2016-03-11 20:54:00.000';
-- invalid timestamp
select timestamp '2016-33-11 20:54:00.000';

-- interval
select interval 13.123456789 seconds, interval -13.123456789 second;
select interval 1 year 2 month 3 week 4 day 5 hour 6 minute 7 seconds 8 millisecond, 9 microsecond;
-- ns is not supported
select interval 10 nanoseconds;

-- unsupported data type
select GEO '(10,-6)';

-- big decimal parsing
select 90912830918230182310293801923652346786BD, 123.0E-28BD, 123.08BD;

-- out of range big decimal
select 1.20E-38BD;

-- hexadecimal binary literal
select x'2379ACFe';

-- invalid hexadecimal binary literal
select X'XuZ';

-- Hive literal_double test.
SELECT 3.14, -3.14, 3.14e8, 3.14e-8, -3.14e8, -3.14e-8, 3.14e+8, 3.14E8, 3.14E-8;
