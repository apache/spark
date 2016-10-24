
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
