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

-- super large scientific notation double literals should still be valid doubles
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
select interval 1 year 2 month 3 week 4 day 5 hour 6 minute 7 seconds 8 millisecond 9 microsecond;
select interval '30' year '25' month '-100' day '40' hour '80' minute '299.889987299' second;
select interval '0 0:0:0.1' day to second;
select interval '10-9' year to month;
select interval '20 15:40:32.99899999' day to hour;
select interval '20 15:40:32.99899999' day to minute;
select interval '20 15:40:32.99899999' day to second;
select interval '15:40:32.99899999' hour to minute;
select interval '15:40.99899999' hour to second;
select interval '15:40' hour to second;
select interval '15:40:32.99899999' hour to second;
select interval '20 40:32.99899999' minute to second;
select interval '40:32.99899999' minute to second;
select interval '40:32' minute to second;
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

-- map + interval test
select map(1, interval 1 day, 2, interval 3 week);

-- typed interval expression
select interval 'interval 3 year 1 hour';
select interval '3 year 1 hour';

-- typed integer expression
select integer '7';
select integer'7';
select integer '2147483648';

-- malformed interval literal
select interval;
select interval 1 fake_unit;
select interval 1 year to month;
select interval '1' year to second;
select interval '10-9' year to month '2-1' year to month;
select interval '10-9' year to month '12:11:10' hour to second;
select interval '1 15:11' day to minute '12:11:10' hour to second;
select interval 1 year '2-1' year to month;
select interval 1 year '12:11:10' hour to second;
select interval '10-9' year to month '1' year;
select interval '12:11:10' hour to second '1' year;
-- malformed interval literal with ansi mode
SET spark.sql.ansi.enabled=true;
select interval;
select interval 1 fake_unit;
select interval 1 year to month;
select 1 year to month;
select interval '1' year to second;
select '1' year to second;
select interval 1 year '2-1' year to month;
select 1 year '2-1' year to month;
