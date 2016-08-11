-- Literal parsing

-- null
select null;

-- boolean
select true, false;

-- byte (tinyint)
select 1Y, 127Y, -127Y;
select 128Y;

-- short (smallint)
select 1S, 32767S, -32767S;
select 32768S;

-- long (bigint)
select 1L, 2147483648L, 9223372036854775807L;
select 9223372036854775808L;

-- integral parsing

-- parse int
select 1, -1;

-- parse int max and min value
select 2147483647, -2147483648;

-- parse as longs (Int.MaxValue + 1, and Int.MinValue - 1)
select 2147483648, -2147483649;

-- parse long max and min value
select 9223372036854775807, -9223372036854775808;

-- parse as decimals (Long.MaxValue + 1, and Long.MinValue - 1)
select 9223372036854775808, -9223372036854775809;

-- double
select 1D, 1.2D, 1e10, 1.5e5, .10D, 0.10D, .1e5;

-- decimal parsing
select 0.3, -0.8, .5, -.18, 0.1111, .1111;

-- super large decimal numbers
select 1234567890123456789012345678901234567890;
select 1234567890123456789012345678901234567890.0;

-- super large scientific notation numbers should still be valid doubles
select 123456789012345678901234567890123456789e10, 123456789012345678901234567890123456789.1e10;

-- string
select "Hello Peter!";
