-- group by all
-- see https://www.linkedin.com/posts/mosha_duckdb-firebolt-snowflake-activity-7009615821006131200-VQ0o

create temporary view data as select * from values
  ("USA", "San Francisco", "Reynold", 1, 11.0),
  ("USA", "San Francisco", "Matei", 2, 12.0),
  ("USA", "Berkeley", "Xiao", 3, 13.0),
  ("China", "Hangzhou", "Wenchen", 4, 14.0),
  ("China", "Shanghai", "Shanghaiese", 5, 15.0),
  ("Korea", "Seoul", "Hyukjin", 6, 16.0)
  as data(country, city, name, id, power);

-- basic
select country, count(*) from data group by *;

-- two grouping columns and two aggregates
select country, city, count(*), sum(power) from data group by *;

-- different ordering
select count(*), country, city, sum(power) from data group by *;

-- alias in grouping column
select country as con, count(*) from data group by *;

-- alias in aggregate column
select country, count(*) as cnt from data group by *;

-- scalar expression in grouping column
select upper(country), count(*) as powerup from data group by *;

-- scalar expression in aggregate column
select country, sum(power) + 10 as powerup from data group by *;

-- group by all without aggregate, which should just become a distinct
select country, city from data group by *;

-- make sure aliases are propagated through correctly
select con, powerup from
  (select country as con, sum(power) + 10 as powerup from data group by *);

-- having
select country, count(id) as cnt from data group by * having cnt > 1;

-- no grouping column
select count(id) from data group by *;

-- a more complex no grouping column case
select count(id + power / 2) * 3 from data group by *;

-- no grouping column on an empty relation
-- this should still return one row because we rewrite this to a global aggregate, as opposed to
-- returning zero row (grouping by a constant).
select count(*) from (select * from data where country = "DNS") group by *;

-- complex cases that we choose not to infer; fail with a useful error message
select id + count(*) from data group by *;

-- an even more complex case that we choose not to infer; fail with a useful error message
select (id + id) / 2 + count(*) * 2 from data group by *;
