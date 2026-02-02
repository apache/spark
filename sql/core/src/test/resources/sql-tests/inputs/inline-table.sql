
-- single row, without table and column alias
select * from values ("one", 1);

-- single row, without column alias
select * from values ("one", 1) as data;

-- single row
select * from values ("one", 1) as data(a, b);

-- single column multiple rows
select * from values 1, 2, 3 as data(a);

-- three rows
select * from values ("one", 1), ("two", 2), ("three", null) as data(a, b);

-- null type
select * from values ("one", null), ("two", null) as data(a, b);

-- int and long coercion
select * from values ("one", 1), ("two", 2L) as data(a, b);

-- foldable expressions
select * from values ("one", 1 + 0), ("two", 1 + 3L) as data(a, b);

-- expressions with alias
select * from values ("one", 1 as one) as data(a, b);

-- literal functions
select a from values ("one", current_timestamp) as data(a, b);

-- complex types
select * from values ("one", array(0, 1)), ("two", array(2, 3)) as data(a, b);

-- decimal and double coercion
select * from values ("one", 2.0), ("two", 3.0D) as data(a, b);

-- error reporting: nondeterministic function rand
select * from values ("one", rand(5)), ("two", 3.0D) as data(a, b);

-- error reporting: different number of columns
select * from values ("one", 2.0), ("two") as data(a, b);

-- error reporting: types that are incompatible
select * from values ("one", array(0, 1)), ("two", struct(1, 2)) as data(a, b);

-- error reporting: number aliases different from number data values
select * from values ("one"), ("two") as data(a, b);

-- error reporting: unresolved expression
select * from values ("one", random_not_exist_func(1)), ("two", 2) as data(a, b);

-- error reporting: aggregate expression
select * from values ("one", count(1)), ("two", 2) as data(a, b);

-- string to timestamp
select * from values (timestamp('1991-12-06 00:00:00.0'), array(timestamp('1991-12-06 01:00:00.0'), timestamp('1991-12-06 12:00:00.0'))) as data(a, b);

-- ReplaceExpressions as row
select * from values (try_add(5, 0));
select * from values (try_divide(5, 0));
select * from values (10 + try_divide(5, 0));

-- now() should be kept as tempResolved inline expression.
select count(distinct ct) from values now(), now(), now() as data(ct);

-- current_timestamp() should be kept as tempResolved inline expression.
select count(distinct ct) from values current_timestamp(), current_timestamp() as data(ct);
