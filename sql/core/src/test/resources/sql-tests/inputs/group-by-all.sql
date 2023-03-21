-- group by all
-- see https://www.linkedin.com/posts/mosha_duckdb-firebolt-snowflake-activity-7009615821006131200-VQ0o

create temporary view data as select * from values
  ("USA", "San Francisco", "Reynold", 1, 11.0),
  ("USA", "San Francisco", "Matei", 2, 12.0),
  ("USA", "Berkeley", "Xiao", 3, 13.0),
  ("China", "Hangzhou", "Wenchen", 4, 14.0),
  ("China", "Shanghai", "Shanghaiese", 5, 15.0),
  ("Korea", "Seoul", "Hyukjin", 6, 16.0),
  ("UK", "London", "Sean", 7, 17.0)
  as data(country, city, name, id, power);

-- basic
select country, count(*) from data group by ALL;

-- different case
select country, count(*) from data group by aLl;

-- a column named "all" would still work
select all, city, count(*) from (select country as all, city, id from data) group by all, city;

-- a column named "all" should take precedence over the normal group by all expansion
-- if all refers to the column, then the following should return 3 rows.
-- if all refers to the global aggregate, then 1 row.
SELECT count(1) FROM VALUES(1), (2), (3) AS T(all) GROUP BY all;

-- two grouping columns and two aggregates
select country, city, count(*), sum(power) from data group by all;

-- different ordering
select count(*), country, city, sum(power) from data group by all;

-- alias in grouping column
select country as con, count(*) from data group by all;


-- alias in aggregate column
select country, count(*) as cnt from data group by all;

-- scalar expression in grouping column
select upper(country), count(*) as powerup from data group by all;

-- scalar expression in aggregate column
select country, sum(power) + 10 as powerup from data group by all;

-- group by all without aggregate, which should just become a distinct
select country, city from data group by all;

-- make sure aliases are propagated through correctly
select con, powerup from
  (select country as con, sum(power) + 10 as powerup from data group by all);

-- having
select country, count(id) as cnt from data group by all having cnt > 1;

-- no grouping column
select count(id) from data group by all;

-- a more complex no grouping column case
select count(id + power / 2) * 3 from data group by all;

-- no grouping column on an empty relation
-- this should still return one row because we rewrite this to a global aggregate, as opposed to
-- returning zero row (grouping by a constant).
select count(*) from (select * from data where country = "DNS") group by all;

-- complex cases that we choose not to infer; fail with a useful error message
select id + count(*) from data group by all;

-- an even more complex case that we choose not to infer; fail with a useful error message
select (id + id) / 2 + count(*) * 2 from data group by all;

-- uncorrelated subquery should work
select country, (select count(*) from data) as cnt, count(id) as cnt_id from data group by all;

-- correlated subquery should also work
select country, (select count(*) from data d1 where d1.country = d2.country), count(id) from data d2 group by all;

-- correlated subquery together with aggregate function doesn't work.
-- make sure we report the right error UNRESOLVED_ALL_IN_GROUP_BY, rather than some random subquery error.
select (select count(*) from data d1 where d1.country = d2.country) + count(id) from data d2 group by all;

-- SELECT list contains unresolved column, should not report UNRESOLVED_ALL_IN_GROUP_BY
select non_exist from data group by all;