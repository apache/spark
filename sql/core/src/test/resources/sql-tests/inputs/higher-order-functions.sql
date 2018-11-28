create or replace temporary view nested as values
  (1, array(32, 97), array(array(12, 99), array(123, 42), array(1))),
  (2, array(77, -76), array(array(6, 96, 65), array(-1, -2))),
  (3, array(12), array(array(17)))
  as t(x, ys, zs);

-- Only allow lambda's in higher order functions.
select upper(x -> x) as v;

-- Identity transform an array
select transform(zs, z -> z) as v from nested;

-- Transform an array
select transform(ys, y -> y * y) as v from nested;

-- Transform an array with index
select transform(ys, (y, i) -> y + i) as v from nested;

-- Transform an array with reference
select transform(zs, z -> concat(ys, z)) as v from nested;

-- Transform an array to an array of 0's
select transform(ys, 0) as v from nested;

-- Transform a null array
select transform(cast(null as array<int>), x -> x + 1) as v;

-- Filter.
select filter(ys, y -> y > 30) as v from nested;

-- Filter a null array
select filter(cast(null as array<int>), y -> true) as v;

-- Filter nested arrays
select transform(zs, z -> filter(z, zz -> zz > 50)) as v from nested;

-- Aggregate.
select aggregate(ys, 0, (y, a) -> y + a + x) as v from nested;

-- Aggregate average.
select aggregate(ys, (0 as sum, 0 as n), (acc, x) -> (acc.sum + x, acc.n + 1), acc -> acc.sum / acc.n) as v from nested;

-- Aggregate nested arrays
select transform(zs, z -> aggregate(z, 1, (acc, val) -> acc * val * size(z))) as v from nested;

-- Aggregate a null array
select aggregate(cast(null as array<int>), 0, (a, y) -> a + y + 1, a -> a + 2) as v;

-- Check for element existence
select exists(ys, y -> y > 30) as v from nested;

-- Check for element existence in a null array
select exists(cast(null as array<int>), y -> y > 30) as v;

-- Zip with array
select zip_with(ys, zs, (a, b) -> a + size(b)) as v from nested;

-- Zip with array with concat
select zip_with(array('a', 'b', 'c'), array('d', 'e', 'f'), (x, y) -> concat(x, y)) as v;

-- Zip with array coalesce
select zip_with(array('a'), array('d', null, 'f'), (x, y) -> coalesce(x, y)) as v;

create or replace temporary view nested as values
  (1, map(1, 1, 2, 2, 3, 3)),
  (2, map(4, 4, 5, 5, 6, 6))
  as t(x, ys);

-- Identity Transform Keys in a map
select transform_keys(ys, (k, v) -> k) as v from nested;

-- Transform Keys in a map by adding constant
select transform_keys(ys, (k, v) -> k + 1) as v from nested;

-- Transform Keys in a map using values
select transform_keys(ys, (k, v) -> k + v) as v from nested;

-- Identity Transform values in a map
select transform_values(ys, (k, v) -> v) as v from nested;

-- Transform values in a map by adding constant
select transform_values(ys, (k, v) -> v + 1) as v from nested;

-- Transform values in a map using values
select transform_values(ys, (k, v) -> k + v) as v from nested;
