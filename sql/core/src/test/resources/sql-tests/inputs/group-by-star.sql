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

-- group by all without group by column
select count(*) from data group by *;

-- group by all without aggregate, which should just become a distinct
select country, city from data group by *;

-- make sure aliases are propagated through correctly
select con, powerup from
  (select country as con, sum(power) + 10 as powerup from data group by *);

-- having
select country, count(*) as cnt from data group by * having cnt > 1;
