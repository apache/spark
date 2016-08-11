-- Verifies how we parse numbers

-- parse as ints
select 1, -1;

-- parse as longs
select 2147483648, -2147483649;

-- parse as decimals
select 9223372036854775808, -9223372036854775809;

-- various floating point (decimal) formats
select 0.3, -0.8, .5, -.18, 0.1111;
