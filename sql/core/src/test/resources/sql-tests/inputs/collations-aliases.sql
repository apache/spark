-- test cases for implicit aliases to collated expression trees are correctly generated

create table t1(s string, utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('Spark', 'Spark', 'SQL');
insert into t1 values ('aaAaAAaA', 'aaAaAAaA', 'aaAaAAaA');
insert into t1 values ('aaAaAAaA', 'aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('aaAaAAaA', 'aaAaAAaA', 'aaAaaAaAaaAaaAaAaaAaaAaA');
insert into t1 values ('bbAbaAbA', 'bbAbAAbA', 'a');
insert into t1 values ('İo', 'İo', 'İo');
insert into t1 values ('İo', 'İo', 'İo ');
insert into t1 values ('İo', 'İo ', 'İo');
insert into t1 values ('İo', 'İo', 'i̇o');
insert into t1 values ('efd2', 'efd2', 'efd2');
insert into t1 values ('Hello, world! Nice day.', 'Hello, world! Nice day.', 'Hello, world! Nice day.');
insert into t1 values ('Something else. Nothing here.', 'Something else. Nothing here.', 'Something else. Nothing here.');
insert into t1 values ('kitten', 'kitten', 'sitTing');
insert into t1 values ('abc', 'abc', 'abc');
insert into t1 values ('abcdcba', 'abcdcba', 'aBcDCbA');

-- Simple select
select concat_ws(' ', utf8_lcase, utf8_lcase) from t1;

-- Select by implicit alias
select `concat_ws(' ' collate UTF8_LCASE, utf8_lcase, utf8_lcase)` from (
  select concat_ws(' ', utf8_lcase, utf8_lcase) from t1
);

-- Select by star
select * from (
  select concat_ws(' ', utf8_lcase, utf8_lcase) from t1
);

-- Select by qualified star
select subq1.* from (
  select concat_ws(' ', utf8_lcase, utf8_lcase) from t1
) AS subq1;

-- Implicit alias in CTE output
with cte as (
  select concat_ws(' ', utf8_lcase, utf8_lcase) from t1
)
select * from cte;

-- Implicit alias in EXISTS subquery output
select * from values (1) where exists (
  select concat_ws(' ', utf8_lcase, utf8_lcase) from t1
);

-- Implicit alias in scalar subquery output
select (
  select concat_ws(' ', utf8_lcase, utf8_lcase) from t1 limit 1
);

-- Scalar subquery with CTE with implicit alias
select (
  with cte as (
    select concat_ws(' ', utf8_lcase, utf8_lcase) from t1
  )
  select * from cte limit 1
);

-- Outer reference to implicit alias
select * from (
  select concat_ws(' ', utf8_lcase, utf8_lcase) from t1 limit 1
)
where (
  `concat_ws(' ' collate UTF8_LCASE, utf8_lcase, utf8_lcase)` == 'aaa'
);

-- Implicit alias reference in Sort
select lower(`concat_ws(' ' collate UTF8_LCASE, utf8_lcase, utf8_lcase)`) from (
  select concat_ws(' ', utf8_lcase, utf8_lcase) from t1
  group by 1
  order by 1
);

-- Implciit alias from aggregate in Sort
select lower(`concat_ws(' ' collate UTF8_LCASE, utf8_lcase, utf8_lcase)`) from (
  select concat_ws(' ', utf8_lcase, utf8_lcase) from t1
  group by 1
  order by max(concat_ws(' ', utf8_lcase, utf8_lcase))
);

-- Implicit alias in view schema
create temporary view v1 as (
  select concat_ws(' ', utf8_lcase, utf8_lcase) from t1
);

select * from v1;

select `concat_ws(' ' collate UTF8_LCASE, utf8_lcase, utf8_lcase)` from v1;

drop view v1;

drop table t1;
