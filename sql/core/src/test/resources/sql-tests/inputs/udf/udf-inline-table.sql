-- This test file was converted from inline-table.sql.
-- [SPARK-28291] UDFs cannot be evaluated within inline table definition
-- TODO: We should add UDFs in VALUES clause when [SPARK-28291] is resolved.

-- single row, without table and column alias
select udf(col1), udf(col2) from values ("one", 1);

-- single row, without column alias
select udf(col1), udf(udf(col2)) from values ("one", 1) as data;

-- single row
select udf(a), b from values ("one", 1) as data(a, b);

-- single column multiple rows
select udf(a) from values 1, 2, 3 as data(a);

-- three rows
select udf(a), b from values ("one", 1), ("two", 2), ("three", null) as data(a, b);

-- null type
select udf(a), b from values ("one", null), ("two", null) as data(a, b);

-- int and long coercion
select udf(a), b from values ("one", 1), ("two", 2L) as data(a, b);

-- foldable expressions
select udf(udf(a)), udf(b) from values ("one", 1 + 0), ("two", 1 + 3L) as data(a, b);

-- complex types
select udf(a), b from values ("one", array(0, 1)), ("two", array(2, 3)) as data(a, b);

-- decimal and double coercion
select udf(a), b from values ("one", 2.0), ("two", 3.0D) as data(a, b);

-- error reporting: nondeterministic function rand
select udf(a), b from values ("one", rand(5)), ("two", 3.0D) as data(a, b);

-- error reporting: different number of columns
select udf(a), udf(b) from values ("one", 2.0), ("two") as data(a, b);

-- error reporting: types that are incompatible
select udf(a), udf(b) from values ("one", array(0, 1)), ("two", struct(1, 2)) as data(a, b);

-- error reporting: number aliases different from number data values
select udf(a), udf(b) from values ("one"), ("two") as data(a, b);

-- error reporting: unresolved expression
select udf(a), udf(b) from values ("one", random_not_exist_func(1)), ("two", 2) as data(a, b);

-- error reporting: aggregate expression
select udf(a), udf(b) from values ("one", count(1)), ("two", 2) as data(a, b);

-- string to timestamp
select udf(a), b from values (timestamp('1991-12-06 00:00:00.0'), array(timestamp('1991-12-06 01:00:00.0'), timestamp('1991-12-06 12:00:00.0'))) as data(a, b);
