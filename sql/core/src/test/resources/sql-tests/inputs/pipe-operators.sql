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
|> inner join join_test_t2 tablesample (100 percent) jt2 using (a);

table join_test_t1
|> inner join (select 1 as a) tablesample (100 percent) jt2 using (a);

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

-- Cleanup.
-----------
drop table t;
drop table other;
drop table st;
