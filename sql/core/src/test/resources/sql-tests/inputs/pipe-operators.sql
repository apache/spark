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

create temporary view courseSales as select * from values
  ("dotNET", 2012, 10000),
  ("Java", 2012, 20000),
  ("dotNET", 2012, 5000),
  ("dotNET", 2013, 48000),
  ("Java", 2013, 30000)
  as courseSales(course, year, earnings);

create temporary view courseEarnings as select * from values
  ("dotNET", 15000, 48000, 22500),
  ("Java", 20000, 30000, NULL)
  as courseEarnings(course, `2012`, `2013`, `2014`);

create temporary view courseEarningsAndSales as select * from values
  ("dotNET", 15000, NULL, 48000, 1, 22500, 1),
  ("Java", 20000, 1, 30000, 2, NULL, NULL)
  as courseEarningsAndSales(
    course, earnings2012, sales2012, earnings2013, sales2013, earnings2014, sales2014);

create temporary view yearsWithComplexTypes as select * from values
  (2012, array(1, 1), map('1', 1), struct(1, 'a')),
  (2013, array(2, 2), map('2', 2), struct(2, 'b'))
  as yearsWithComplexTypes(y, a, m, s);

create temporary view join_test_t1 as select * from values (1) as grouping(a);
create temporary view join_test_t2 as select * from values (1) as grouping(a);
create temporary view join_test_t3 as select * from values (1) as grouping(a);
create temporary view join_test_empty_table as select a from join_test_t2 where false;

create temporary view lateral_test_t1(c1, c2)
  as values (0, 1), (1, 2);
create temporary view lateral_test_t2(c1, c2)
  as values (0, 2), (0, 3);
create temporary view lateral_test_t3(c1, c2)
  as values (0, array(0, 1)), (1, array(2)), (2, array()), (null, array(4));
create temporary view lateral_test_t4(c1, c2)
  as values (0, 1), (0, 2), (1, 1), (1, 3);

create temporary view natural_join_test_t1 as select * from values
  ("one", 1), ("two", 2), ("three", 3) as natural_join_test_t1(k, v1);

create temporary view natural_join_test_t2 as select * from values
  ("one", 1), ("two", 22), ("one", 5) as natural_join_test_t2(k, v2);

create temporary view natural_join_test_t3 as select * from values
  ("one", 4), ("two", 5), ("one", 6) as natural_join_test_t3(k, v3);

create temporary view windowTestData as select * from values
  (null, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "a"),
  (1, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "a"),
  (1, 2L, 2.5D, date("2017-08-02"), timestamp_seconds(1502000000), "a"),
  (2, 2147483650L, 100.001D, date("2020-12-31"), timestamp_seconds(1609372800), "a"),
  (1, null, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "b"),
  (2, 3L, 3.3D, date("2017-08-03"), timestamp_seconds(1503000000), "b"),
  (3, 2147483650L, 100.001D, date("2020-12-31"), timestamp_seconds(1609372800), "b"),
  (null, null, null, null, null, null),
  (3, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), null)
  AS testData(val, val_long, val_double, val_date, val_timestamp, cate);

-- FROM operators: positive tests.
----------------------------------

-- FromClause alone.
from t;

-- Table alone.
table t;

-- Selecting from a constant.
from t
|> select 1 as x;

-- Selecting using a table alias.
from t as t_alias
|> select t_alias.x;

-- Selecting using a table alias.
from t as t_alias
|> select t_alias.x as tx, t_alias.y as ty
|> where ty = 'def'
|> select tx;

-- Selecting from multiple relations.
from t, other
|> select t.x + other.a as z;

-- Selecting from multiple relations with join.
from t join other on (t.x = other.a)
|> select t.x + other.a as z;

-- Selecting from lateral view.
from t lateral view explode(array(100, 101)) as ly
|> select t.x + ly as z;

-- Selecting struct fields.
from st
|> select col.i1;

-- Selecting struct fields using a table alias.
from st as st_alias
|> select st_alias.col.i1;

-- Selecting from a VALUES list.
from values (0), (1) tab(col)
|> select col as x;

-- FROM operators: negative tests.
----------------------------------

-- It is not possible to use the FROM operator accepting an input relation.
from t
|> from t;

-- SELECT operators: positive tests.
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

-- Pipe operator SELECT inside expression subqueries.
select (values (0) tab(col) |> select col) as result;

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

-- SELECT * is supported.
table t
|> select *;

table t
|> select * except (y);

-- Hints are supported.
table t
|> select /*+ repartition(3) */ *;

table t
|> select /*+ repartition(3) */ distinct x;

table t
|> select /*+ repartition(3) */ all x;

-- SELECT operators: negative tests.
---------------------------------------

-- Aggregate functions are not allowed in the pipe operator SELECT list.
table t
|> select sum(x) as result;

table t
|> select y, length(y) + sum(x) as result;

-- EXTEND operators: positive tests.
------------------------------------

-- Extending with a constant.
table t
|> extend 1 as z;

-- Extending without an explicit alias.
table t
|> extend 1;

-- Extending with an attribute.
table t
|> extend x as z;

-- Extending with an expression.
table t
|> extend x + length(y) as z;

-- Extending two times.
table t
|> extend x + length(y) as z, x + 1 as zz;

-- Extending two times in sequence.
table t
|> extend x + length(y) as z
|> extend z + 1 as zz;

-- Extending with a struct field.
select col from st
|> extend col.i1 as z;

-- Extending with a subquery.
table t
|> extend (select a from other where x = a limit 1) as z;

-- Extending with a correlated reference.
table t
|> where exists (
    table other
    |> extend t.x
    |> select * except (a, b));

-- Extending with a column name that already exists in the input relation.
table t
|> extend 1 as x;

-- Window functions are allowed in the pipe operator EXTEND list.
table t
|> extend first_value(x) over (partition by y) as result;

-- Lateral column aliases in the pipe operator EXTEND list.
table t
|> extend x + length(y) as z, z + 1 as plus_one;

-- EXTEND operators: negative tests.
------------------------------------

-- Aggregations are not allowed.
table t
|> extend sum(x) as z;

-- DISTINCT is not supported.
table t
|> extend distinct x as z;

-- EXTEND * is not supported.
table t
|> extend *;

-- SET operators: positive tests.
---------------------------------

-- Setting with a constant.
-- The indicated column is not the last column in the table, and the SET operator will replace it
-- with the new value in its existing position.
table t
|> set x = 1;

-- Setting with an attribute.
table t
|> set y = x;

-- Setting with an expression.
table t
|> extend 1 as z
|> set z = x + length(y);

-- Setting two times.
table t
|> extend 1 as z
|> extend 2 as zz
|> set z = x + length(y), zz = x + 1;

table other
|> extend 3 as c
|> set a = b, b = c;

-- Setting two times with a lateral reference.
table t
|> extend 1 as z
|> extend 2 as zz
|> set z = x + length(y), zz = z + 1;

-- Setting two times in sequence.
table t
|> extend 1 as z
|> set z = x + length(y)
|> set z = z + 1;

-- SET assignments with duplicate keys. This is supported, and we can update the column as we go.
table t
|> extend 1 as z
|> set z = x + length(y), z = z + 1;

-- Setting with a struct field.
select col from st
|> extend 1 as z
|> set z = col.i1;

-- Setting with a subquery.
table t
|> set y = (select a from other where x = a limit 1);

-- Setting with a backquoted column name with a dot inside.
table t
|> extend 1 as `x.y.z`
|> set `x.y.z` = x + length(y);

-- Window functions are allowed in the pipe operator SET list.
table t
|> extend 1 as z
|> set z = first_value(x) over (partition by y);

-- Any prior table aliases remain visible after a SET operator.
values (0), (1) lhs(a)
|> inner join values (1), (2) rhs(a) using (a)
|> extend lhs.a + rhs.a as z1
|> extend lhs.a - rhs.a as z2
|> drop z1
|> where z2 = 0
|> order by lhs.a, rhs.a, z2
|> set z2 = 4
|> limit 2
|> select lhs.a, rhs.a, z2;

-- SET operators: negative tests.
---------------------------------

-- SET with a column name that does not exist in the input relation.
table t
|> set z = 1;

-- SET with an alias.
table t
|> set x = 1 as z;

-- Setting nested fields in structs is not supported.
select col from st
|> set col.i1 = 42;

-- DROP operators: positive tests.
------------------------------------

-- Dropping a column.
table t
|> drop y;

-- Dropping two times.
select 1 as x, 2 as y, 3 as z
|> drop z, y;

-- Dropping two times in sequence.
select 1 as x, 2 as y, 3 as z
|> drop z
|> drop y;

-- Dropping all columns in the input relation.
select x from t
|> drop x;

-- Dropping a backquoted column name with a dot inside.
table t
|> extend 1 as `x.y.z`
|> drop `x.y.z`;

-- DROP operators: negative tests.
----------------------------------

-- Dropping a column that is not present in the input relation.
table t
|> drop z;

-- Attempting to drop a struct field.
table st
|> drop col.i1;

table st
|> drop `col.i1`;

-- Duplicate fields in the drop list.
select 1 as x, 2 as y, 3 as z
|> drop z, y, z;

-- AS operators: positive tests.
--------------------------------

-- Renaming a table.
table t
|> as u
|> select u.x, u.y;

-- Renaming an input relation that is not a table.
select 1 as x, 2 as y
|> as u
|> select u.x, u.y;

-- Renaming as a backquoted name including a period.
table t
|> as `u.v`
|> select `u.v`.x, `u.v`.y;

-- Renaming two times.
table t
|> as u
|> as v
|> select v.x, v.y;

-- Filtering by referring to the table or table subquery alias.
table t
|> as u
|> where u.x = 1;

-- AS operators: negative tests.
--------------------------------

-- Multiple aliases are not supported.
table t
|> as u, v;

-- Expressions are not supported.
table t
|> as 1 + 2;

-- Renaming as an invalid name.
table t
|> as u-v;

table t
|> as u@v;

table t
|> as u#######v;

-- WHERE operators: positive tests.
-----------------------------------

-- Filtering with a constant predicate.
table t
|> where true;

-- Filtering with a predicate based on attributes from the input relation.
table t
|> where x + length(y) < 4;

-- Two consecutive filters are allowed.
table t
|> where x + length(y) < 4
|> where x + length(y) < 3;

-- It is possible to use the WHERE operator instead of the HAVING clause when processing the result
-- of aggregations. For example, this WHERE operator is equivalent to the normal SQL "HAVING x = 1".
(select x, sum(length(y)) as sum_len from t group by x)
|> where x = 1;

-- Filtering by referring to the table or table subquery alias.
table t
|> where t.x = 1;

table t
|> where spark_catalog.default.t.x = 1;

-- Filtering using struct fields.
(select col from st)
|> where col.i1 = 1;

table st
|> where st.col.i1 = 2;

-- Expression subqueries in the WHERE clause.
table t
|> where exists (select a from other where x = a limit 1);

-- Aggregations are allowed within expression subqueries in the pipe operator WHERE clause as long
-- no aggregate functions exist in the top-level expression predicate.
table t
|> where (select any_value(a) from other where x = a limit 1) = 1;

-- WHERE operators: negative tests.
-----------------------------------

-- Aggregate functions are not allowed in the top-level WHERE predicate.
-- (Note: to implement this behavior, perform the aggregation first separately and then add a
-- pipe-operator WHERE clause referring to the result of aggregate expression(s) therein).
table t
|> where sum(x) = 1;

table t
|> where y = 'abc' or length(y) + sum(x) = 1;

-- Window functions are not allowed in the WHERE clause (pipe operators or otherwise).
-- (Note: to implement this behavior, perform the window function first separately in a SELECT
-- clause and then add a pipe-operator WHERE clause referring to the result of the window function
-- expression(s) therein).
table t
|> where sum(x) over (partition by y) = 1;

table t
|> where sum(x) over w = 1
   window w as (partition by y);

select * from t where sum(x) over (partition by y) = 1;

-- Pipe operators may only refer to attributes produced as output from the directly-preceding
-- pipe operator, not from earlier ones.
table t
|> select x, length(y) as z
|> where x + length(y) < 4;

table t
|> select x, length(y) as z
|> limit 1000
|> where x + length(y) < 4;

table t
|> select x, length(y) as z
|> limit 1000 offset 1
|> where x + length(y) < 4;

table t
|> select x, length(y) as z
|> order by x, y
|> where x + length(y) < 4;

-- If the WHERE clause wants to filter rows produced by an aggregation, it is not valid to try to
-- refer to the aggregate functions directly; it is necessary to use aliases instead.
(select x, sum(length(y)) as sum_len from t group by x)
|> where sum(length(y)) = 3;

-- Pivot and unpivot operators: positive tests.
-----------------------------------------------

table courseSales
|> select `year`, course, earnings
|> pivot (
     sum(earnings)
     for course in ('dotNET', 'Java')
  );

table courseSales
|> select `year` as y, course as c, earnings as e
|> pivot (
     sum(e) as s, avg(e) as a
     for y in (2012 as firstYear, 2013 as secondYear)
   );

-- Pivot on multiple pivot columns with aggregate columns of complex data types.
select course, `year`, y, a
from courseSales
join yearsWithComplexTypes on `year` = y
|> pivot (
     max(a)
     for (y, course) in ((2012, 'dotNET'), (2013, 'Java'))
   );

-- Pivot on pivot column of struct type.
select earnings, `year`, s
from courseSales
join yearsWithComplexTypes on `year` = y
|> pivot (
     sum(earnings)
     for s in ((1, 'a'), (2, 'b'))
   );

table courseEarnings
|> unpivot (
     earningsYear for `year` in (`2012`, `2013`, `2014`)
   );

table courseEarnings
|> unpivot include nulls (
     earningsYear for `year` in (`2012`, `2013`, `2014`)
   );

table courseEarningsAndSales
|> unpivot include nulls (
     (earnings, sales) for `year` in (
       (earnings2012, sales2012) as `2012`,
       (earnings2013, sales2013) as `2013`,
       (earnings2014, sales2014) as `2014`)
   );

-- Pivot and unpivot operators: negative tests.
-----------------------------------------------

-- The PIVOT operator refers to a column 'year' is not available in the input relation.
table courseSales
|> select course, earnings
|> pivot (
     sum(earnings)
     for `year` in (2012, 2013)
   );

-- Non-literal PIVOT values are not supported.
table courseSales
|> pivot (
     sum(earnings)
     for `year` in (course, 2013)
   );

-- The PIVOT and UNPIVOT clauses are mutually exclusive.
table courseSales
|> select course, earnings
|> pivot (
     sum(earnings)
     for `year` in (2012, 2013)
   )
   unpivot (
     earningsYear for `year` in (`2012`, `2013`, `2014`)
   );

table courseSales
|> select course, earnings
|> unpivot (
     earningsYear for `year` in (`2012`, `2013`, `2014`)
   )
   pivot (
     sum(earnings)
     for `year` in (2012, 2013)
   );

-- Multiple PIVOT and/or UNPIVOT clauses are not supported in the same pipe operator.
table courseSales
|> select course, earnings
|> pivot (
     sum(earnings)
     for `year` in (2012, 2013)
   )
   pivot (
     sum(earnings)
     for `year` in (2012, 2013)
   );

table courseSales
|> select course, earnings
|> unpivot (
     earningsYear for `year` in (`2012`, `2013`, `2014`)
   )
   unpivot (
     earningsYear for `year` in (`2012`, `2013`, `2014`)
   )
   pivot (
     sum(earnings)
     for `year` in (2012, 2013)
   );

-- Sampling operators: positive tests.
--------------------------------------

-- We will use the REPEATABLE clause and/or adjust the sampling options to either remove no rows or
-- all rows to help keep the tests deterministic.
table t
|> tablesample (100 percent) repeatable (0);

table t
|> tablesample (2 rows) repeatable (0);

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
|> tablesample (-100 percent) repeatable (0);

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

-- JOIN operators: positive tests.
----------------------------------

table join_test_t1
|> inner join join_test_empty_table;

table join_test_t1
|> cross join join_test_empty_table;

table join_test_t1
|> left outer join join_test_empty_table;

table join_test_t1
|> right outer join join_test_empty_table;

table join_test_t1
|> full outer join join_test_empty_table using (a);

table join_test_t1
|> full outer join join_test_empty_table on (join_test_t1.a = join_test_empty_table.a);

table join_test_t1
|> left semi join join_test_empty_table;

table join_test_t1
|> left anti join join_test_empty_table;

select * from join_test_t1 where true
|> inner join join_test_empty_table;

select 1 as x, 2 as y
|> inner join (select 1 as x, 4 as y) using (x);

table join_test_t1
|> inner join (join_test_t2 jt2 inner join join_test_t3 jt3 using (a)) using (a)
|> select a, join_test_t1.a, jt2.a, jt3.a;

table join_test_t1
|> inner join join_test_t2 tablesample (100 percent) repeatable (0) jt2 using (a);

table join_test_t1
|> inner join (select 1 as a) tablesample (100 percent) repeatable (0) jt2 using (a);

table join_test_t1
|> join join_test_t1 using (a);

-- Lateral joins.
table lateral_test_t1
|> join lateral (select c1);

table lateral_test_t1
|> join lateral (select c1 from lateral_test_t2);

table lateral_test_t1
|> join lateral (select lateral_test_t1.c1 from lateral_test_t2);

table lateral_test_t1
|> join lateral (select lateral_test_t1.c1 + t2.c1 from lateral_test_t2 t2);

table lateral_test_t1
|> join lateral (select *);

table lateral_test_t1
|> join lateral (select * from lateral_test_t2);

table lateral_test_t1
|> join lateral (select lateral_test_t1.* from lateral_test_t2);

table lateral_test_t1
|> join lateral (select lateral_test_t1.*, t2.* from lateral_test_t2 t2);

table lateral_test_t1
|> join lateral_test_t2
|> join lateral (select lateral_test_t1.c2 + lateral_test_t2.c2);

-- Natural joins.
table natural_join_test_t1
|> natural join natural_join_test_t2
|> where k = "one";

table natural_join_test_t1
|> natural join natural_join_test_t2 nt2
|> select natural_join_test_t1.*;

table natural_join_test_t1
|> natural join natural_join_test_t2 nt2
|> natural join natural_join_test_t3 nt3
|> select natural_join_test_t1.*, nt2.*, nt3.*;

-- JOIN operators: negative tests.
----------------------------------

-- Multiple joins within the same pipe operator are not supported without parentheses.
table join_test_t1
|> inner join join_test_empty_table
   inner join join_test_empty_table;

-- The join pipe operator can only refer to column names from the previous relation.
table join_test_t1
|> select 1 + 2 as result
|> full outer join join_test_empty_table on (join_test_t1.a = join_test_empty_table.a);

-- The table from the pipe input is not visible as a table name in the right side.
table join_test_t1 jt
|> cross join (select * from jt);

-- Set operations: positive tests.
----------------------------------

-- Union all.
table t
|> union all table t;

-- Union distinct.
table t
|> union table t;

-- Union all with a table subquery.
(select * from t)
|> union all table t;

-- Union distinct with a table subquery.
(select * from t)
|> union table t;

-- Union all with a VALUES list.
values (0, 'abc') tab(x, y)
|> union all table t;

-- Union distinct with a VALUES list.
-- The |> WHERE operator applies to the result of the |> UNION operator, not to the "table t" input.
values (2, 'xyz') tab(x, y)
|> union table t
|> where x = 0;

-- Union distinct with a VALUES list.
-- The |> DROP operator applies to the result of the |> UNION operator, not to the "table t" input.
values (2, 'xyz') tab(x, y)
|> union table t
|> drop x;

-- Union all with a table subquery on both the source and target sides.
(select * from t)
|> union all (select * from t);

-- Except all.
table t
|> except all table t;

-- Except distinct.
table t
|> except table t;

-- Intersect all.
table t
|> intersect all table t;

-- Intersect distinct.
table t
|> intersect table t;

-- Minus all.
table t
|> minus all table t;

-- Minus distinct.
table t
|> minus table t;

-- Set operations: negative tests.
----------------------------------

-- The UNION operator requires the same number of columns in the input relations.
table t
|> select x
|> union all table t;

-- The UNION operator requires the column types to be compatible.
table t
|> union all table st;

-- Sorting and repartitioning operators: positive tests.
--------------------------------------------------------

-- Order by.
table t
|> order by x;

-- Order by with a table subquery.
(select * from t)
|> order by x;

-- Order by with a VALUES list.
values (0, 'abc') tab(x, y)
|> order by x;

-- Limit.
table t
|> order by x
|> limit 1;

-- Limit with offset.
table t
|> where x = 1
|> select y
|> limit 2 offset 1;

-- Offset is allowed without limit.
table t
|> where x = 1
|> select y
|> offset 1;

-- LIMIT ALL and OFFSET 0 are equivalent to no LIMIT or OFFSET clause, respectively.
table t
|> limit all offset 0;

-- Distribute by.
table t
|> distribute by x;

-- Cluster by.
table t
|> cluster by x;

-- Sort and distribute by.
table t
|> sort by x distribute by x;

-- It is possible to apply a final ORDER BY clause on the result of a query containing pipe
-- operators.
table t
|> order by x desc
order by y;

-- Sorting and repartitioning operators: negative tests.
--------------------------------------------------------

-- Multiple order by clauses are not supported in the same pipe operator.
-- We add an extra "ORDER BY y" clause at the end in this test to show that the "ORDER BY x + y"
-- clause was consumed end the of the final query, not as part of the pipe operator.
table t
|> order by x desc order by x + y
order by y;

-- The ORDER BY clause may only refer to column names from the previous input relation.
table t
|> select 1 + 2 as result
|> order by x;

-- The DISTRIBUTE BY clause may only refer to column names from the previous input relation.
table t
|> select 1 + 2 as result
|> distribute by x;

-- Combinations of multiple ordering and limit clauses are not supported.
table t
|> order by x limit 1;

-- ORDER BY and SORT BY are not supported at the same time.
table t
|> order by x sort by x;

-- Aggregation operators: positive tests.
-----------------------------------------

-- Basic aggregation with a GROUP BY clause. The resulting table contains all the attributes from
-- the grouping keys followed by all the attributes from the aggregate functions, in order.
table other
|> aggregate sum(b) as result group by a;

-- Basic aggregation with a GROUP BY clause, followed by a SELECT of just the aggregate function.
-- This restricts the output attributes to just the aggregate function.
table other
|> aggregate sum(b) as result group by a
|> select result;

-- Basic aggregation with a GROUP BY clause, followed by a SELECT of just the grouping expression.
-- This restricts the output attributes to just the grouping expression. Note that we must use an
-- alias for the grouping expression to refer to it in the SELECT clause.
table other
|> aggregate sum(b) group by a + 1 as gkey
|> select gkey;

-- Basic aggregation on a constant table.
select 1 as x, 2 as y
|> aggregate group by x, y;

-- Basic aggregation with group by ordinals.
select 3 as x, 4 as y
|> aggregate group by 1, 2;

values (3, 4) as tab(x, y)
|> aggregate sum(y) group by 1;

values (3, 4), (5, 4) as tab(x, y)
|> aggregate sum(y) group by 1;

select 3 as x, 4 as y
|> aggregate sum(y) group by 1, 1;

select 1 as `1`, 2 as `2`
|> aggregate sum(`2`) group by `1`;

select 3 as x, 4 as y
|> aggregate sum(y) group by 2;

select 3 as x, 4 as y, 5 as z
|> aggregate sum(y) group by 2;

select 3 as x, 4 as y, 5 as z
|> aggregate sum(y) group by 3;

select 3 as x, 4 as y, 5 as z
|> aggregate sum(y) group by 2, 3;

select 3 as x, 4 as y, 5 as z
|> aggregate sum(y) group by 1, 2, 3;

select 3 as x, 4 as y, 5 as z
|> aggregate sum(y) group by x, 2, 3;

-- Basic table aggregation.
table t
|> aggregate sum(x);

-- Basic table aggregation with an alias.
table t
|> aggregate sum(x) + 1 as result_plus_one;

-- Grouping with no aggregate functions.
table other
|> aggregate group by a
|> where a = 1;

-- Group by an expression on columns, all of which are already grouped.
select 1 as x, 2 as y, 3 as z
|> aggregate group by x, y, x + y as z;

-- Group by an expression on columns, some of which (y) aren't already grouped.
select 1 as x, 2 as y, 3 as z
|> aggregate group by x as z, x + y as z;

-- We get an output column for each item in GROUP BY, even when they are duplicate expressions.
select 1 as x, 2 as y, named_struct('z', 3) as st
|> aggregate group by x, y, x, x, st.z, (st).z, 1 + x, 2 + x;

-- Chained aggregates.
select 1 x, 2 y, 3 z
|> aggregate sum(z) z group by x, y
|> aggregate avg(z) z group by x
|> aggregate count(distinct z) c;

-- Ambiguous name from duplicate GROUP BY item. This is generally allowed.
select 1 x, 3 z
|> aggregate count(*) group by x, z, x
|> select x;

-- Aggregate expressions may contain a mix of aggregate functions and grouping expressions.
table other
|> aggregate a + count(b) group by a;

-- Aggregation operators: negative tests.
-----------------------------------------

-- All aggregate expressions must contain at least one aggregate function.
table other
|> aggregate a group by a;

-- GROUP BY ALL is not currently supported.
select 3 as x, 4 as y
|> aggregate group by all;

-- GROUP BY ROLLUP is not supported yet.
table courseSales
|> aggregate sum(earnings) group by rollup(course, `year`)
|> where course = 'dotNET' and `year` = '2013';

-- GROUP BY CUBE is not supported yet.
table courseSales
|> aggregate sum(earnings) group by cube(course, `year`)
|> where course = 'dotNET' and `year` = '2013';

-- GROUPING SETS is not supported yet.
table courseSales
|> aggregate sum(earnings) group by course, `year` grouping sets(course, `year`)
|> where course = 'dotNET' and `year` = '2013';

-- GROUPING/GROUPING_ID is not supported yet.
table courseSales
|> aggregate sum(earnings), grouping(course) + 1
   group by course
|> where course = 'dotNET' and `year` = '2013';

-- GROUPING/GROUPING_ID is not supported yet.
table courseSales
|> aggregate sum(earnings), grouping_id(course)
   group by course
|> where course = 'dotNET' and `year` = '2013';

-- GROUP BY () is not valid syntax.
select 1 as x, 2 as y
|> aggregate group by ();

-- Non-aggregate expressions are not allowed in place of aggregate functions.
table other
|> aggregate a;

-- Using aggregate functions without the AGGREGATE keyword is not allowed.
table other
|> select sum(a) as result;

-- The AGGREGATE keyword requires a GROUP BY clause and/or aggregation function(s).
table other
|> aggregate;

-- The AGGREGATE GROUP BY list cannot be empty.
table other
|> aggregate group by;

-- The AGGREGATE keyword is required to perform grouping.
table other
|> group by a;

-- Window functions are not allowed in the AGGREGATE expression list.
table other
|> aggregate sum(a) over () group by b;

-- Ambiguous name from AGGREGATE list vs GROUP BY.
select 1 x, 2 y, 3 z
|> aggregate count(*) AS c, sum(x) AS x group by x
|> where c = 1
|> where x = 1;

-- WINDOW operators (within SELECT): positive tests.
---------------------------------------------------

-- SELECT with a WINDOW clause.
table windowTestData
|> select cate, sum(val) over w
   window w as (partition by cate order by val);

-- SELECT with RANGE BETWEEN as part of the window definition.
table windowTestData
|> select cate, sum(val) over w
   window w as (order by val_timestamp range between unbounded preceding and current row);

-- SELECT with a WINDOW clause not being referred in the SELECT list.
table windowTestData
|> select cate, val
    window w as (partition by cate order by val);

-- multiple SELECT clauses, each with a WINDOW clause (with the same window definition names).
table windowTestData
|> select cate, val, sum(val) over w as sum_val
   window w as (partition by cate)
|> select cate, val, sum_val, first_value(cate) over w
   window w as (order by val);

-- SELECT with a WINDOW clause for multiple window definitions.
table windowTestData
|> select cate, val, sum(val) over w1, first_value(cate) over w2
   window w1 as (partition by cate), w2 as (order by val);

-- SELECT with a WINDOW clause for multiple window functions over one window definition
table windowTestData
|> select cate, val, sum(val) over w, first_value(val) over w
   window w1 as (partition by cate order by val);

-- SELECT with a WINDOW clause, using struct fields.
(select col from st)
|> select col.i1, sum(col.i2) over w
   window w as (partition by col.i1 order by col.i2);

table st
|> select st.col.i1, sum(st.col.i2) over w
   window w as (partition by st.col.i1 order by st.col.i2);

table st
|> select spark_catalog.default.st.col.i1, sum(spark_catalog.default.st.col.i2) over w
   window w as (partition by spark_catalog.default.st.col.i1 order by spark_catalog.default.st.col.i2);

-- SELECT with one WINDOW definition shadowing a column name.
table windowTestData
|> select cate, sum(val) over val
   window val as (partition by cate order by val);

-- WINDOW operators (within SELECT): negative tests.
---------------------------------------------------

-- WINDOW without definition is not allowed in the pipe operator SELECT clause.
table windowTestData
|> select cate, sum(val) over w;

-- Multiple WINDOW clauses are not supported in the pipe operator SELECT clause.
table windowTestData
|> select cate, val, sum(val) over w1, first_value(cate) over w2
   window w1 as (partition by cate)
   window w2 as (order by val);

-- WINDOW definition cannot be referred across different pipe operator SELECT clauses.
table windowTestData
|> select cate, val, sum(val) over w as sum_val
   window w as (partition by cate order by val)
|> select cate, val, sum_val, first_value(cate) over w;

table windowTestData
|> select cate, val, first_value(cate) over w as first_val
|> select cate, val, sum(val) over w as sum_val
   window w as (order by val);

-- Exercise SQL compilation using a subset of TPC-DS table schemas.
-------------------------------------------------------------------

-- Q1
with customer_total_return as
(select
    sr_customer_sk as ctr_customer_sk,
    sr_store_sk as ctr_store_sk,
    sum(sr_return_amt) as ctr_total_return
  from store_returns, date_dim
  where sr_returned_date_sk = d_date_sk and d_year = 2000
  group by sr_customer_sk, sr_store_sk)
select c_customer_id
from customer_total_return ctr1, store, customer
where ctr1.ctr_total_return >
  (select avg(ctr_total_return) * 1.2
  from customer_total_return ctr2
  where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  and s_store_sk = ctr1.ctr_store_sk
  and s_state = 'tn'
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100;

with customer_total_return as
  (from store_returns
  |> join date_dim
  |> where sr_returned_date_sk = d_date_sk and d_year = 2000
  |> aggregate sum(sr_return_amt) as ctr_total_return
       group by sr_customer_sk as ctr_customer_sk, sr_store_sk as ctr_store_sk)
from customer_total_return ctr1
|> join store
|> join customer
|> where ctr1.ctr_total_return >
     (table customer_total_return
      |> as ctr2
      |> where ctr1.ctr_store_sk = ctr2.ctr_store_sk
      |> aggregate avg(ctr_total_return) * 1.2)
     and s_store_sk = ctr1.ctr_store_sk
     and s_state = 'tn'
     and ctr1.ctr_customer_sk = c_customer_sk
|> order by c_customer_id
|> limit 100
|> select c_customer_id;

-- Q2
with wscs as
( select
    sold_date_sk,
    sales_price
  from (select
    ws_sold_date_sk sold_date_sk,
    ws_ext_sales_price sales_price
  from web_sales) x
  union all
  (select
    cs_sold_date_sk sold_date_sk,
    cs_ext_sales_price sales_price
  from catalog_sales)),
    wswscs as
  ( select
    d_week_seq,
    sum(case when (d_day_name = 'sunday')
      then sales_price
        else null end)
    sun_sales,
    sum(case when (d_day_name = 'monday')
      then sales_price
        else null end)
    mon_sales,
    sum(case when (d_day_name = 'tuesday')
      then sales_price
        else null end)
    tue_sales,
    sum(case when (d_day_name = 'wednesday')
      then sales_price
        else null end)
    wed_sales,
    sum(case when (d_day_name = 'thursday')
      then sales_price
        else null end)
    thu_sales,
    sum(case when (d_day_name = 'friday')
      then sales_price
        else null end)
    fri_sales,
    sum(case when (d_day_name = 'saturday')
      then sales_price
        else null end)
    sat_sales
  from wscs, date_dim
  where d_date_sk = sold_date_sk
  group by d_week_seq)
select
  d_week_seq1,
  round(sun_sales1 / sun_sales2, 2),
  round(mon_sales1 / mon_sales2, 2),
  round(tue_sales1 / tue_sales2, 2),
  round(wed_sales1 / wed_sales2, 2),
  round(thu_sales1 / thu_sales2, 2),
  round(fri_sales1 / fri_sales2, 2),
  round(sat_sales1 / sat_sales2, 2)
from
  (select
    wswscs.d_week_seq d_week_seq1,
    sun_sales sun_sales1,
    mon_sales mon_sales1,
    tue_sales tue_sales1,
    wed_sales wed_sales1,
    thu_sales thu_sales1,
    fri_sales fri_sales1,
    sat_sales sat_sales1
  from wswscs, date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and d_year = 2001) y,
  (select
    wswscs.d_week_seq d_week_seq2,
    sun_sales sun_sales2,
    mon_sales mon_sales2,
    tue_sales tue_sales2,
    wed_sales wed_sales2,
    thu_sales thu_sales2,
    fri_sales fri_sales2,
    sat_sales sat_sales2
  from wswscs, date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and d_year = 2001 + 1) z
where d_week_seq1 = d_week_seq2 - 53
order by d_week_seq1;

with wscs as
  (table web_sales
  |> select
       ws_sold_date_sk sold_date_sk,
       ws_ext_sales_price sales_price
  |> as x
  |> union all (
       table catalog_sales
       |> select
            cs_sold_date_sk sold_date_sk,
            cs_ext_sales_price sales_price)
  |> select
       sold_date_sk,
       sales_price),
wswscs as
  (table wscs
  |> join date_dim
  |> where d_date_sk = sold_date_sk
  |> aggregate
      sum(case when (d_day_name = 'sunday')
        then sales_price
          else null end)
      sun_sales,
      sum(case when (d_day_name = 'monday')
        then sales_price
          else null end)
      mon_sales,
      sum(case when (d_day_name = 'tuesday')
        then sales_price
          else null end)
      tue_sales,
      sum(case when (d_day_name = 'wednesday')
        then sales_price
          else null end)
      wed_sales,
      sum(case when (d_day_name = 'thursday')
        then sales_price
          else null end)
      thu_sales,
      sum(case when (d_day_name = 'friday')
        then sales_price
          else null end)
      fri_sales,
      sum(case when (d_day_name = 'saturday')
        then sales_price
          else null end)
      sat_sales
      group by d_week_seq)
table wswscs
|> join date_dim
|> where date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001
|> select
     wswscs.d_week_seq d_week_seq1,
     sun_sales sun_sales1,
     mon_sales mon_sales1,
     tue_sales tue_sales1,
     wed_sales wed_sales1,
     thu_sales thu_sales1,
     fri_sales fri_sales1,
     sat_sales sat_sales1
|> as y
|> join (
     table wswscs
     |> join date_dim
     |> where date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001 + 1
     |> select
          wswscs.d_week_seq d_week_seq2,
          sun_sales sun_sales2,
          mon_sales mon_sales2,
          tue_sales tue_sales2,
          wed_sales wed_sales2,
          thu_sales thu_sales2,
          fri_sales fri_sales2,
          sat_sales sat_sales2
     |> as z)
|> where d_week_seq1 = d_week_seq2 - 53
|> order by d_week_seq1
|> select
     d_week_seq1,
     round(sun_sales1 / sun_sales2, 2),
     round(mon_sales1 / mon_sales2, 2),
     round(tue_sales1 / tue_sales2, 2),
     round(wed_sales1 / wed_sales2, 2),
     round(thu_sales1 / thu_sales2, 2),
     round(fri_sales1 / fri_sales2, 2),
     round(sat_sales1 / sat_sales2, 2);

-- Q3
select
  dt.d_year,
  item.i_brand_id brand_id,
  item.i_brand brand,
  sum(ss_ext_sales_price) sum_agg
from date_dim dt, store_sales, item
where dt.d_date_sk = store_sales.ss_sold_date_sk
  and store_sales.ss_item_sk = item.i_item_sk
  and item.i_manufact_id = 128
  and dt.d_moy = 11
group by dt.d_year, item.i_brand, item.i_brand_id
order by dt.d_year, sum_agg desc, brand_id
limit 100;

table date_dim
|> as dt
|> join store_sales
|> join item
|> where dt.d_date_sk = store_sales.ss_sold_date_sk
     and store_sales.ss_item_sk = item.i_item_sk
     and item.i_manufact_id = 128
     and dt.d_moy = 11
|> aggregate sum(ss_ext_sales_price) sum_agg
     group by dt.d_year d_year, item.i_brand_id brand_id, item.i_brand brand
|> order by d_year, sum_agg desc, brand_id
|> limit 100;

-- Q12
select
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  sum(ws_ext_sales_price) as itemrevenue,
  sum(ws_ext_sales_price) * 100 / sum(sum(ws_ext_sales_price))
  over
  (partition by i_class) as revenueratio
from
  web_sales, item, date_dim
where
  ws_item_sk = i_item_sk
    and i_category in ('sports', 'books', 'home')
    and ws_sold_date_sk = d_date_sk
    and d_date between cast('1999-02-22' as date)
  and (cast('1999-02-22' as date) + interval 30 days)
group by
  i_item_id, i_item_desc, i_category, i_class, i_current_price
order by
  i_category, i_class, i_item_id, i_item_desc, revenueratio
limit 100;

table web_sales
|> join item
|> join date_dim
|> where ws_item_sk = i_item_sk
     and i_category in ('sports', 'books', 'home')
     and ws_sold_date_sk = d_date_sk
     and d_date between cast('1999-02-22' as date)
     and (cast('1999-02-22' as date) + interval 30 days)
|> aggregate sum(ws_ext_sales_price) AS itemrevenue
     group by i_item_id, i_item_desc, i_category, i_class, i_current_price
|> extend
     itemrevenue * 100 / sum(itemrevenue)
       over (partition by i_class) as revenueratio
|> order by i_category, i_class, i_item_id, i_item_desc, revenueratio
|> select i_item_desc, i_category, i_class, i_current_price, itemrevenue, revenueratio
|> limit 100;

-- Q44
select
  asceding.rnk,
  i1.i_product_name best_performing,
  i2.i_product_name worst_performing
from (select *
from (select
  item_sk,
  rank()
  over (
    order by rank_col asc) rnk
from (select
  ss_item_sk item_sk,
  avg(ss_net_profit) rank_col
from store_sales ss1
where ss_store_sk = 4
group by ss_item_sk
having avg(ss_net_profit) > 0.9 * (select avg(ss_net_profit) rank_col
from store_sales
where ss_store_sk = 4
  and ss_addr_sk is null
group by ss_store_sk)) v1) v11
where rnk < 11) asceding,
  (select *
  from (select
    item_sk,
    rank()
    over (
      order by rank_col desc) rnk
  from (select
    ss_item_sk item_sk,
    avg(ss_net_profit) rank_col
  from store_sales ss1
  where ss_store_sk = 4
  group by ss_item_sk
  having avg(ss_net_profit) > 0.9 * (select avg(ss_net_profit) rank_col
  from store_sales
  where ss_store_sk = 4
    and ss_addr_sk is null
  group by ss_store_sk)) v2) v21
  where rnk < 11) descending,
  item i1, item i2
where asceding.rnk = descending.rnk
  and i1.i_item_sk = asceding.item_sk
  and i2.i_item_sk = descending.item_sk
order by asceding.rnk
limit 100;

from store_sales ss1
|> where ss_store_sk = 4
|> aggregate avg(ss_net_profit) rank_col
     group by ss_item_sk as item_sk
|> where rank_col > 0.9 * (
     from store_sales
     |> where ss_store_sk = 4
          and ss_addr_sk is null
     |> aggregate avg(ss_net_profit) rank_col
          group by ss_store_sk
     |> select rank_col)
|> as v1
|> select
     item_sk,
     rank() over (
       order by rank_col asc) rnk
|> as v11
|> where rnk < 11
|> as asceding
|> join (
     from store_sales ss1
     |> where ss_store_sk = 4
     |> aggregate avg(ss_net_profit) rank_col
          group by ss_item_sk as item_sk
     |> where rank_col > 0.9 * (
          table store_sales
          |> where ss_store_sk = 4
               and ss_addr_sk is null
          |> aggregate avg(ss_net_profit) rank_col
               group by ss_store_sk
          |> select rank_col)
     |> as v2
     |> select
          item_sk,
          rank() over (
            order by rank_col asc) rnk
     |> as v21
     |> where rnk < 11) descending
|> join item i1
|> join item i2
|> where asceding.rnk = descending.rnk
     and i1.i_item_sk = asceding.item_sk
     and i2.i_item_sk = descending.item_sk
|> order by asceding.rnk
|> select
     asceding.rnk,
     i1.i_product_name best_performing,
     i2.i_product_name worst_performing;

-- Q51
with web_v1 as (
  select
    ws_item_sk item_sk,
    d_date,
    sum(sum(ws_sales_price))
    over (partition by ws_item_sk
      order by d_date
      rows between unbounded preceding and current row) cume_sales
  from web_sales, date_dim
  where ws_sold_date_sk = d_date_sk
    and d_month_seq between 1200 and 1200 + 11
    and ws_item_sk is not null
  group by ws_item_sk, d_date),
    store_v1 as (
    select
      ss_item_sk item_sk,
      d_date,
      sum(sum(ss_sales_price))
      over (partition by ss_item_sk
        order by d_date
        rows between unbounded preceding and current row) cume_sales
    from store_sales, date_dim
    where ss_sold_date_sk = d_date_sk
      and d_month_seq between 1200 and 1200 + 11
      and ss_item_sk is not null
    group by ss_item_sk, d_date)
select *
from (select
  item_sk,
  d_date,
  web_sales,
  store_sales,
  max(web_sales)
  over (partition by item_sk
    order by d_date
    rows between unbounded preceding and current row) web_cumulative,
  max(store_sales)
  over (partition by item_sk
    order by d_date
    rows between unbounded preceding and current row) store_cumulative
from (select
  case when web.item_sk is not null
    then web.item_sk
  else store.item_sk end item_sk,
  case when web.d_date is not null
    then web.d_date
  else store.d_date end d_date,
  web.cume_sales web_sales,
  store.cume_sales store_sales
from web_v1 web full outer join store_v1 store on (web.item_sk = store.item_sk
  and web.d_date = store.d_date)
     ) x) y
where web_cumulative > store_cumulative
order by item_sk, d_date
limit 100;

with web_v1 as (
  table web_sales
  |> join date_dim
  |> where ws_sold_date_sk = d_date_sk
       and d_month_seq between 1200 and 1200 + 11
       and ws_item_sk is not null
  |> aggregate sum(ws_sales_price) as sum_ws_sales_price
       group by ws_item_sk as item_sk, d_date
  |> extend sum(sum_ws_sales_price)
       over (partition by item_sk
         order by d_date
         rows between unbounded preceding and current row)
       as cume_sales),
store_v1 as (
  table store_sales
  |> join date_dim
  |> where ss_sold_date_sk = d_date_sk
       and d_month_seq between 1200 and 1200 + 11
       and ss_item_sk is not null
  |> aggregate sum(ss_sales_price) as sum_ss_sales_price
       group by ss_item_sk as item_sk, d_date
  |> extend sum(sum_ss_sales_price)
       over (partition by item_sk
           order by d_date
           rows between unbounded preceding and current row)
       as cume_sales)
table web_v1
|> as web
|> full outer join store_v1 store
     on (web.item_sk = store.item_sk and web.d_date = store.d_date)
|> select
     case when web.item_sk is not null
       then web.item_sk
       else store.item_sk end item_sk,
     case when web.d_date is not null
       then web.d_date
       else store.d_date end d_date,
     web.cume_sales web_sales,
     store.cume_sales store_sales
|> as x
|> select
     item_sk,
     d_date,
     web_sales,
     store_sales,
     max(web_sales)
       over (partition by item_sk
         order by d_date
         rows between unbounded preceding and current row) web_cumulative,
     max(store_sales)
       over (partition by item_sk
         order by d_date
         rows between unbounded preceding and current row) store_cumulative
|> as y
|> where web_cumulative > store_cumulative
|> order by item_sk, d_date
|> limit 100;

-- Cleanup.
-----------
drop table t;
drop table other;
drop table st;
