-- Verifies how we parse numbers

-- parse as ints
select 1, -1;

-- parse as longs (Int.MaxValue + 1, and Int.MinValue - 1)
select 2147483648, -2147483649;

-- parse long min and max value
select 9223372036854775807, -9223372036854775808;

-- parse as decimals (Long.MaxValue + 1, and Long.MinValue - 1)
select 9223372036854775808, -9223372036854775809;

-- various floating point (decimal) formats
select 0.3, -0.8, .5, -.18, 0.1111;
