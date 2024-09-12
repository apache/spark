-- Prepare some test data.
--------------------------
drop table if exists t;
create table t(x int, y string) using csv;
insert into t values (0, 'abc'), (1, 'def');

drop table if exists other;
create table other(a int, b int) using json;
insert into other values (1, 1), (1, 2), (2, 4);

drop table if exists st;
create table st(x int, col struct<i1:int, i2:int>) using parquet;
insert into st values (1, (2, 3));

-- Selection operators: positive tests.
---------------------------------------

-- Selecting a constant.
table t
|> select 1 as x;

-- Selecting attributes.
table t
|> select x, y;

-- Chained pipe SELECT operators.
table t
|> select x, y
|> select x + length(y) as z;

-- Using the VALUES list as the source relation.
values (0), (1) tab(col)
|> select col * 2 as result;

-- Using a table subquery as the source relation.
(select * from t union all select * from t)
|> select x + length(y) as result;

-- Enclosing the result of a pipe SELECT operation in a table subquery.
(table t
 |> select x, y
 |> select x)
union all
select x from t where x < 1;

-- Selecting struct fields.
(select col from st)
|> select col.i1;

table st
|> select st.col.i1;

-- Expression subqueries in the pipe operator SELECT list.
table t
|> select (select a from other where x = a limit 1) as result;

-- Aggregations are allowed within expression subqueries in the pipe operator SELECT list as long as
-- no aggregate functions exist in the top-level select list.
table t
|> select (select any_value(a) from other where x = a limit 1) as result;

-- Lateral column aliases in the pipe operator SELECT list.
table t
|> select x + length(x) as z, z + 1 as plus_one;

-- Window functions are allowed in the pipe operator SELECT list.
table t
|> select first_value(x) over (partition by y) as result;

select 1 x, 2 y, 3 z
|> select 1 + sum(x) over (),
     avg(y) over (),
     x,
     avg(x+1) over (partition by y order by z) AS a2
|> select a2;

table t
|> select x, count(*) over ()
|> select x;

-- DISTINCT is supported.
table t
|> select distinct x, y;

-- Selection operators: negative tests.
---------------------------------------

-- Aggregate functions are not allowed in the pipe operator SELECT list.
table t
|> select sum(x) as result;

table t
|> select y, length(y) + sum(x) as result;

-- Sampling operators: positive tests.
--------------------------------------

-- We will use the REPEATABLE clause and/or adjust the sampling options to either remove no rows or
-- all rows to help keep the tests deterministic.
table t
|> tablesample (100 percent) repeatable (0);

table t
|> tablesample (1 rows) repeatable (0);

table t
|> tablesample (bucket 1 out of 1) repeatable (0);

table t
|> tablesample (100 percent) repeatable (0)
|> tablesample (5 rows) repeatable (0)
|> tablesample (bucket 1 out of 1) repeatable (0);

-- Sampling operators: negative tests.
--------------------------------------

-- The sampling method is required.
table t
|> tablesample ();

-- Negative sampling options are not supported.
table t
|> tablesample (-100 percent);

table t
|> tablesample (-5 rows);

-- The sampling method may not refer to attribute names from the input relation.
table t
|> tablesample (x rows);

-- The bucket number is invalid.
table t
|> tablesample (bucket 2 out of 1);

-- Byte literals are not supported.
table t
|> tablesample (200b) repeatable (0);

-- Invalid byte literal syntax.
table t
|> tablesample (200) repeatable (0);

-- Cleanup.
-----------
drop table t;
drop table other;
drop table st;
