-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
-- Window Functions Testing
-- https://github.com/postgres/postgres/blob/REL_12_STABLE/src/test/regress/sql/window.sql#L320-562

-- Test window operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

CREATE TABLE empsalary (
    depname string,
    empno integer,
    salary int,
    enroll_date date
) USING parquet;

INSERT INTO empsalary VALUES
  ('develop', 10, 5200, date '2007-08-01'),
  ('sales', 1, 5000, date '2006-10-01'),
  ('personnel', 5, 3500, date '2007-12-10'),
  ('sales', 4, 4800, date '2007-08-08'),
  ('personnel', 2, 3900, date '2006-12-23'),
  ('develop', 7, 4200, date '2008-01-01'),
  ('develop', 9, 4500, date '2008-01-01'),
  ('sales', 3, 4800, date '2007-08-01'),
  ('develop', 8, 6000, date '2006-10-01'),
  ('develop', 11, 5200, date '2007-08-15');

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- CREATE TEMP VIEW v_window AS
-- SELECT i, min(i) over (order by i range between '1 day' preceding and '10 days' following) as min_i
--   FROM range(now(), now()+'100 days', '1 hour') i;

-- RANGE offset PRECEDING/FOLLOWING tests

SELECT sum(unique1) over (order by four range between 2 preceding and 1 preceding),
unique1, four
FROM tenk1 WHERE unique1 < 10;

SELECT sum(unique1) over (order by four desc range between 2 preceding and 1 preceding),
unique1, four
FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (order by four range between 2 preceding and 1 preceding exclude no others),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (order by four range between 2 preceding and 1 preceding exclude current row),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (order by four range between 2 preceding and 1 preceding exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (order by four range between 2 preceding and 1 preceding exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (order by four range between 2 preceding and 6 following exclude ties),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (order by four range between 2 preceding and 6 following exclude group),
-- unique1, four
-- FROM tenk1 WHERE unique1 < 10;

SELECT sum(unique1) over (partition by four order by unique1 range between 5 preceding and 6 following),
unique1, four
FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- SELECT sum(unique1) over (partition by four order by unique1 range between 5 preceding and 6 following
--   exclude current row),unique1, four
-- FROM tenk1 WHERE unique1 < 10;

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select sum(salary) over (order by enroll_date range between '1 year' preceding and '1 year' following),
-- salary, enroll_date from empsalary;

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select sum(salary) over (order by enroll_date desc range between '1 year' preceding and '1 year' following),
-- salary, enroll_date from empsalary;

-- [SPARK-28429] SQL Datetime util function being casted to double instead of timestamp
-- select sum(salary) over (order by enroll_date desc range between '1 year' following and '1 year' following),
-- salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select sum(salary) over (order by enroll_date range between '1 year' preceding and '1 year' following
--   exclude current row), salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select sum(salary) over (order by enroll_date range between '1 year' preceding and '1 year' following
--   exclude group), salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select sum(salary) over (order by enroll_date range between '1 year' preceding and '1 year' following
--   exclude ties), salary, enroll_date from empsalary;

-- [SPARK-28310] ANSI SQL grammar support: first_value/last_value(expression, [RESPECT NULLS | IGNORE NULLS])
-- select first_value(salary) over(order by salary range between 1000 preceding and 1000 following),
-- lead(salary) over(order by salary range between 1000 preceding and 1000 following),
-- nth_value(salary, 1) over(order by salary range between 1000 preceding and 1000 following),
-- salary from empsalary;

-- [SPARK-30734] AnalysisException that window RangeFrame not match RowFrame
-- select last(salary) over(order by salary range between 1000 preceding and 1000 following),
-- lag(salary) over(order by salary range between 1000 preceding and 1000 following),
-- salary from empsalary;

-- [SPARK-28310] ANSI SQL grammar support: first_value/last_value(expression, [RESPECT NULLS | IGNORE NULLS])
-- select first_value(salary) over(order by salary range between 1000 following and 3000 following
--   exclude current row),
-- lead(salary) over(order by salary range between 1000 following and 3000 following exclude ties),
-- nth_value(salary, 1) over(order by salary range between 1000 following and 3000 following
--   exclude ties),
-- salary from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select last(salary) over(order by salary range between 1000 following and 3000 following
--   exclude group),
-- lag(salary) over(order by salary range between 1000 following and 3000 following exclude group),
-- salary from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select first(salary) over(order by enroll_date range between unbounded preceding and '1 year' following
--   exclude ties),
-- last(salary) over(order by enroll_date range between unbounded preceding and '1 year' following),
-- salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select first(salary) over(order by enroll_date range between unbounded preceding and '1 year' following
--   exclude ties),
-- last(salary) over(order by enroll_date range between unbounded preceding and '1 year' following
--   exclude ties),
-- salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select first(salary) over(order by enroll_date range between unbounded preceding and '1 year' following
--   exclude group),
-- last(salary) over(order by enroll_date range between unbounded preceding and '1 year' following
--   exclude group),
-- salary, enroll_date from empsalary;

-- [SPARK-28428] Spark `exclude` always expecting `()`
-- select first(salary) over(order by enroll_date range between unbounded preceding and '1 year' following
--   exclude current row),
-- last(salary) over(order by enroll_date range between unbounded preceding and '1 year' following
--   exclude current row),
-- salary, enroll_date from empsalary;

-- RANGE offset PRECEDING/FOLLOWING with null values
select ss.id, ss.y,
       first(ss.y) over w,
       last(ss.y) over w
from
  (select x.id, x.id as y from range(1,6) as x
   union all select null, 42
   union all select null, 43) ss
window w as
  (order by ss.id asc nulls first range between 2 preceding and 2 following);

select ss.id, ss.y,
       first(ss.y) over w,
       last(ss.y) over w
from
  (select x.id, x.id as y from range(1,6) as x
   union all select null, 42
   union all select null, 43) ss
window w as
  (order by ss.id asc nulls last range between 2 preceding and 2 following);

select ss.id, ss.y,
       first(ss.y) over w,
       last(ss.y) over w
from
  (select x.id, x.id as y from range(1,6) as x
   union all select null, 42
   union all select null, 43) ss
window w as
  (order by ss.id desc nulls first range between 2 preceding and 2 following);

select ss.id, ss.y,
       first(ss.y) over w,
       last(ss.y) over w
from
  (select x.id, x.id as y from range(1,6) as x
   union all select null, 42
   union all select null, 43) ss
window w as
  (order by ss.id desc nulls last range between 2 preceding and 2 following);

-- Check overflow behavior for various integer sizes

select x.id, last(x.id) over (order by x.id range between current row and 2147450884 following)
from range(32764, 32767) x;

select x.id, last(x.id) over (order by x.id desc range between current row and 2147450885 following)
from range(-32766, -32765) x;

select x.id, last(x.id) over (order by x.id range between current row and 4 following)
from range(2147483644, 2147483647) x;

select x.id, last(x.id) over (order by x.id desc range between current row and 5 following)
from range(-2147483646, -2147483645) x;

select x.id, last(x.id) over (order by x.id range between current row and 4 following)
from range(9223372036854775804, 9223372036854775807) x;

select x.id, last(x.id) over (order by x.id desc range between current row and 5 following)
from range(-9223372036854775806, -9223372036854775805) x;

-- Test in_range for other numeric datatypes

create table numerics (
    id int,
    f_float4 float,
    f_float8 float,
    f_numeric int
) using parquet;

insert into numerics values
(1, -3, -3, -3),
(2, -1, -1, -1),
(3, 0, 0, 0),
(4, 1.1, 1.1, 1.1),
(5, 1.12, 1.12, 1.12),
(6, 2, 2, 2),
(7, 100, 100, 100);
-- (8, 'infinity', 'infinity', '1000'),
-- (9, 'NaN', 'NaN', 'NaN'),
-- (0, '-infinity', '-infinity', '-1000');  -- numeric type lacks infinities

select id, f_float4, first(id) over w, last(id) over w
from numerics
window w as (order by f_float4 range between
             1 preceding and 1 following);

select id, f_float4, first(id) over w, last(id) over w
from numerics
window w as (order by f_float4 range between
             1 preceding and 1.1 following);

select id, f_float4, first(id) over w, last(id) over w
from numerics
window w as (order by f_float4 range between
             'inf' preceding and 'inf' following);

select id, f_float4, first(id) over w, last(id) over w
from numerics
window w as (order by f_float4 range between
             1.1 preceding and 'NaN' following);  -- error, NaN disallowed

select id, f_float8, first(id) over w, last(id) over w
from numerics
window w as (order by f_float8 range between
             1 preceding and 1 following);

select id, f_float8, first(id) over w, last(id) over w
from numerics
window w as (order by f_float8 range between
             1 preceding and 1.1 following);

select id, f_float8, first(id) over w, last(id) over w
from numerics
window w as (order by f_float8 range between
             'inf' preceding and 'inf' following);

select id, f_float8, first(id) over w, last(id) over w
from numerics
window w as (order by f_float8 range between
             1.1 preceding and 'NaN' following);  -- error, NaN disallowed

select id, f_numeric, first(id) over w, last(id) over w
from numerics
window w as (order by f_numeric range between
             1 preceding and 1 following);

select id, f_numeric, first(id) over w, last(id) over w
from numerics
window w as (order by f_numeric range between
             1 preceding and 1.1 following);

select id, f_numeric, first(id) over w, last(id) over w
from numerics
window w as (order by f_numeric range between
             1 preceding and 1.1 following);  -- currently unsupported

select id, f_numeric, first(id) over w, last(id) over w
from numerics
window w as (order by f_numeric range between
             1.1 preceding and 'NaN' following);  -- error, NaN disallowed

drop table empsalary;
drop table numerics;
