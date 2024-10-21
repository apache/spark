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
table t
|> where first_value(x) over (partition by y) = 1;

select * from t where first_value(x) over (partition by y) = 1;

-- Pipe operators may only refer to attributes produced as output from the directly-preceding
-- pipe operator, not from earlier ones.
table t
|> select x, length(y) as z
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
values (0, 1) tab(x, y)
|> union table t;

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

-- The WINDOW clause is not supported yet.
table windowTestData
|> window w as (partition by cte order by val)
|> select cate, sum(val) over w;

-- WINDOW and LIMIT are not supported at the same time.
table windowTestData
|> window w as (partition by cate order by val) limit 5;

-- Aggregation operators: positive tests.
-----------------------------------------

-- Basic aggregation on a constant table.
select 1 as x, 2 as y
|> aggregate group by x, y;

-- Basic aggregation with group by ordinals.
select 3 as x, 4 as y
|> aggregate group by 1, 2;

-- Basic aggregation with a GROUP BY clause.
table other
|> aggregate sum(b) as result group by a;

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

-- Aggregation operators: negative tests.
-----------------------------------------

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

-- Non-aggregate expressions are not allowed in place of aggregate functions, even if they appear
-- separately in the GROUP BY clause.
table other
|> aggregate a group by a;

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

-- Cleanup.
-----------
drop table t;
drop table other;
drop table st;
