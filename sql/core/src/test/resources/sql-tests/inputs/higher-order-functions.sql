-- Test higher order functions with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

create or replace temporary view nested as values
  (1, array(32, 97), array(array(12, 99), array(123, 42), array(1))),
  (2, array(77, -76), array(array(6, 96, 65), array(-1, -2))),
  (3, array(12), array(array(17)))
  as t(x, ys, zs);

-- Only allow lambda's in higher order functions.
select upper(x -> x) as v;
-- Also test functions registered with `ExpressionBuilder`.
select ceil(x -> x) as v;

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

-- alias for Aggregate.
select reduce(ys, 0, (y, a) -> y + a + x) as v from nested;
select reduce(ys, (0 as sum, 0 as n), (acc, x) -> (acc.sum + x, acc.n + 1), acc -> acc.sum / acc.n) as v from nested;
select transform(zs, z -> reduce(z, 1, (acc, val) -> acc * val * size(z))) as v from nested;
select reduce(cast(null as array<int>), 0, (a, y) -> a + y + 1, a -> a + 2) as v;

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

-- use non reversed keywords: all is non reversed only if !ansi
select transform(ys, all -> all * all) as v from values (array(32, 97)) as t(ys);
select transform(ys, (all, i) -> all + i) as v from values (array(32, 97)) as t(ys);

-- SPARK-32819: Aggregate on nested string arrays
select aggregate(split('abcdefgh',''), array(array('')), (acc, x) -> array(array(x)));

-- HigherOrderFunctions without lambda variables

select aggregate(array(1, 2, 3), 0, 100) as aggregate_int_literal;
select aggregate(array(1, 2, 3), map(), map('result', 999)) as aggregate_map_literal;
select aggregate(array(1, 2, 3), struct('init', 0), struct('final', 999)) as aggregate_struct_literal;
select aggregate(array(1, 2, 3), array(), array('result')) as aggregate_array_literal;

select array_sort(array(3, 1, 2), 1) as array_sort_int_literal;
select array_sort(array(3, 1, 2), map('compare', 0)) as array_sort_map_literal;
select array_sort(array(3, 1, 2), struct('result', 0)) as array_sort_struct_literal;
select array_sort(array(3, 1, 2), array(0)) as array_sort_array_literal;

select exists(array(1, 2, 3), 1) as exists_int_literal;
select exists(array(1, 2, 3), map('found', true)) as exists_map_literal;
select exists(array(1, 2, 3), struct('exists', true)) as exists_struct_literal;
select exists(array(1, 2, 3), array(true)) as exists_array_literal;

select filter(array(1, 2, 3), 1) as filter_int_literal;
select filter(array(1, 2, 3), map('key', 'value')) as filter_map_literal;
select filter(array(1, 2, 3), struct('valid', true)) as filter_struct_literal;
select filter(array(1, 2, 3), array(true, false)) as filter_array_literal;

select forall(array(1, 2, 3), 1) as forall_int_literal;
select forall(array(1, 2, 3), map('all', true)) as forall_map_literal;
select forall(array(1, 2, 3), struct('all', true)) as forall_struct_literal;
select forall(array(1, 2, 3), array(true, true)) as forall_array_literal;

select map_filter(map('a', 1, 'b', 2), 1) as map_filter_int_literal;
select map_filter(map('a', 1, 'b', 2), map('keep', true)) as map_filter_map_literal;
select map_filter(map('a', 1, 'b', 2), struct('filter', true)) as map_filter_struct_literal;
select map_filter(map('a', 1, 'b', 2), array(true)) as map_filter_array_literal;

select map_zip_with(map('a', 1), map('a', 10), 100) as map_zipwith_int_literal;
select map_zip_with(map('a', 1), map('a', 10), map('merged', true)) as map_zipwith_map_literal;
select map_zip_with(map('a', 1), map('a', 10), struct('left', 1, 'right', 10)) as map_zipwith_struct_literal;
select map_zip_with(map('a', 1), map('a', 10), array('combined')) as map_zipwith_array_literal;

select reduce(array(1, 2, 3), 0, 100) as reduce_int_literal;
select reduce(array(1, 2, 3), map(), map('result', 999)) as reduce_map_literal;
select reduce(array(1, 2, 3), struct('init', 0), struct('final', 999)) as reduce_struct_literal;
select reduce(array(1, 2, 3), array(), array('result')) as reduce_array_literal;

select transform(array(1, 2, 3), 42) as transform_int_literal;
select transform(array(1, 2, 3), map('key', 'value')) as transform_map_literal;
select transform(array(1, 2, 3), struct('id', 99, 'name', 'test')) as transform_struct_literal;
select transform(array(1, 2, 3), array('a', 'b')) as transform_array_literal;

select transform_keys(map('a', 1, 'b', 2), 42) as transform_keys_int_literal;
select transform_keys(map('a', 1, 'b', 2), map('new', 'key')) as transform_keys_map_literal;
select transform_keys(map('a', 1, 'b', 2), struct('key', 'value')) as transform_keys_struct_literal;
select transform_keys(map('a', 1, 'b', 2), array('new_key')) as transform_keys_array_literal;

select transform_values(map('a', 1, 'b', 2), 999) as transform_values_int_literal;
select transform_values(map('a', 1, 'b', 2), map('new', 'value')) as transform_values_map_literal;
select transform_values(map('a', 1, 'b', 2), struct('val', 999)) as transform_values_struct_literal;
select transform_values(map('a', 1, 'b', 2), array('new_value')) as transform_values_array_literal;

select zip_with(array(1, 2, 3), array(4, 5, 6), 100) as zipwith_int_literal;
select zip_with(array(1, 2, 3), array(4, 5, 6), map('merged', true)) as zipwith_map_literal;
select zip_with(array(1, 2, 3), array(4, 5, 6), struct('left', 1, 'right', 2)) as zipwith_struct_literal;
select zip_with(array(1, 2, 3), array(4, 5, 6), array('combined')) as zipwith_array_literal;
