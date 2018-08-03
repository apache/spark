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
