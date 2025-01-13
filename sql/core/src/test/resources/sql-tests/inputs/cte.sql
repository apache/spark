create temporary view t as select * from values 0, 1, 2 as t(id);
create temporary view t2 as select * from values 0, 1 as t(id);
create temporary view t3 as select * from t;

-- WITH clause should not fall into infinite loop by referencing self
WITH s AS (SELECT 1 FROM s) SELECT * FROM s;

WITH r AS (SELECT (SELECT * FROM r))
SELECT * FROM r;

-- WITH clause should reference the base table
WITH t AS (SELECT 1 FROM t) SELECT * FROM t;

-- Table `t` referenced by a view should take precedence over the top CTE `t`
WITH t AS (SELECT 1) SELECT * FROM t3;

-- WITH clause should not allow cross reference
WITH s1 AS (SELECT 1 FROM s2), s2 AS (SELECT 1 FROM s1) SELECT * FROM s1, s2;

-- WITH clause should reference the previous CTE
WITH t1 AS (SELECT * FROM t2), t2 AS (SELECT 2 FROM t1) SELECT * FROM t1 cross join t2;

-- SPARK-18609 CTE with self-join
WITH CTE1 AS (
  SELECT b.id AS id
  FROM   T2 a
         CROSS JOIN (SELECT id AS id FROM T2) b
)
SELECT t1.id AS c1,
       t2.id AS c2
FROM   CTE1 t1
       CROSS JOIN CTE1 t2;

-- CTE with column alias
WITH t(x) AS (SELECT 1)
SELECT * FROM t WHERE x = 1;

-- CTE with multiple column aliases
WITH t(x, y) AS (SELECT 1, 2)
SELECT * FROM t WHERE x = 1 AND y = 2;

-- CTE with duplicate column aliases
WITH t(x, x) AS (SELECT 1, 2)
SELECT * FROM t;

-- CTE with empty column alias list is not allowed
WITH t() AS (SELECT 1)
SELECT * FROM t;

-- CTEs with duplicate names are not allowed
WITH
  t(x) AS (SELECT 1),
  t(x) AS (SELECT 2)
SELECT * FROM t;

-- invalid CTE relation should fail the query even if it's not referenced
WITH t AS (SELECT 1 FROM non_existing_table)
SELECT 2;

-- The following tests are ported from Postgres
-- Multiple uses are evaluated only once
SELECT count(*) FROM (
  WITH q1(x) AS (SELECT random() FROM range(1, 5))
    SELECT * FROM q1
  UNION
    SELECT * FROM q1
) ss;

-- Deeply nested
WITH w1(c1) AS
 (WITH w2(c2) AS
  (WITH w3(c3) AS
   (WITH w4(c4) AS
    (WITH w5(c5) AS
     (WITH w6(c6) AS
      (WITH w7(c7) AS
       (WITH w8(c8) AS
        (SELECT 1)
        SELECT * FROM w8)
       SELECT * FROM w7)
      SELECT * FROM w6)
     SELECT * FROM w5)
    SELECT * FROM w4)
   SELECT * FROM w3)
  SELECT * FROM w2)
SELECT * FROM w1;

-- CTE referencing an outer-level variable, should fail
SELECT ( WITH cte(foo) AS ( VALUES(id) )
         SELECT (SELECT foo FROM cte) )
FROM t;

-- CTE name collision with subquery name
WITH same_name AS (SELECT 42)
SELECT * FROM same_name, (SELECT 10) AS same_name;

-- CTE name collision with subquery name, should fail
WITH same_name(x) AS (SELECT 42)
SELECT same_name.x FROM (SELECT 10) AS same_name(x), same_name;

-- Test behavior with an unknown-type literal in the WITH
WITH q AS (SELECT 'foo' AS x)
SELECT x, typeof(x) FROM q;

-- The following tests are ported from ZetaSQL
-- Alias inside the with hides the underlying column name, should fail
with cte as (select id as id_alias from t)
select id from cte;

-- Reference of later WITH, should fail.
with r1 as (select * from r2),
     r2 as (select 1)
select 2;

-- WITH in a table subquery
SELECT * FROM
  (WITH q AS (select 1 x) SELECT x+1 AS y FROM q);

-- WITH in an expression subquery
select (with q as (select 1 x) select * from q);

-- WITH in an IN subquery
select 1 in (with q as (select 1) select * from q);

-- WITH alias referenced outside its scope, should fail
SELECT * FROM
  (WITH q AS (select 1 x) SELECT x+1 AS y FROM q),
  q;

-- References to CTEs of the same name should be resolved properly
WITH T1 as (select 1 a)
select *
from
  T1 x,
  (WITH T1 as (select 2 b) select * from T1) y,
  T1 z;

-- References to CTEs of the same name should be resolved properly
WITH TTtt as (select 1 a),
     `tTTt_2` as (select 2 a)
select *
from
  (WITH TtTt as (select 3 c) select * from ttTT, `tttT_2`);

-- Correlated CTE subquery
select
  (WITH q AS (select T.x) select * from q)
from (select 1 x, 2 y) T;

-- The main query inside WITH can be correlated.
select
  (WITH q AS (select 3 z) select x + t.y + z from q)
from (select 1 x, 2 y) T;

-- A WITH subquery alias is visible inside a WITH clause subquery.
WITH q1 as (select 1 x)
select * from
  (with q2 as (select * from q1) select * from q2);

-- A WITH subquery alias is visible inside a WITH clause subquery, and they have the same name.
WITH q1 as (select 1 x)
select * from
  (with q1 as (select x+1 from q1) select * from q1);

-- The following tests are ported from DuckDB
-- Duplicate CTE alias, should fail
with cte1 as (select 42), cte1 as (select 42) select * FROM cte1;

-- Refer to CTE in subquery
with cte1 as (Select id as j from t)
select * from cte1 where j = (select max(j) from cte1 as cte2);

-- Nested CTE views that re-use CTE aliases
with cte AS (SELECT * FROM va) SELECT * FROM cte;

-- Self-refer to non-existent cte, should fail.
with cte as (select * from cte) select * from cte;

-- Clean up
DROP VIEW IF EXISTS t;
DROP VIEW IF EXISTS t2;
DROP VIEW IF EXISTS t3;
