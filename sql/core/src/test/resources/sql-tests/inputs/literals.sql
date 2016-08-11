-- Literal parsing

-- null
select null;

-- boolean
select true, false;

-- byte
select 1Y, 1 Y, 127 Y, -127 Y;
select 128Y;

-- short
select 1S, 1 S, 32767 S, -32767 S;
select 32768 S;

-- long
select 1L, 1 L, 2147483648 L, 9223372036854775807 L;
select 9223372036854775808 L;

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
select 1D, 1 D, 1.2D, 1e10, 1.5e5;

-- decimal parsing
select 0.3, -0.8, .5, -.18, 0.1111;

-- string
select "Hello Peter!";
