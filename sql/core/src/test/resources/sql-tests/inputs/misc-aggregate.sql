-- test for misc aggregate functions

CREATE TEMPORARY VIEW empty_table as
    SELECT col1, col2 FROM values (0, "str") as tab(col1, col2) WHERE false;

-- should return non-empty set with nulls
select first(col1), first(col2) from empty_table;
select first(col1) from empty_table;
select first(col1) ignore nulls from empty_table;
select first(col1), first(col2) ignore nulls from empty_table;
