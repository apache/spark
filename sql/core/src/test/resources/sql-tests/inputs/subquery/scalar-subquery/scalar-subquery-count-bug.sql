--CONFIG_DIM1 spark.sql.optimizer.decorrelateInnerQuery.enabled=true
--CONFIG_DIM1 spark.sql.optimizer.decorrelateInnerQuery.enabled=false

create temp view l (a, b)
as values
    (1, 2.0),
    (1, 2.0),
    (2, 1.0),
    (2, 1.0),
    (3, 3.0),
    (null, null),
    (null, 5.0),
    (6, null);

create temp view r (c, d)
as values
    (2, 3.0),
    (2, 3.0),
    (3, 2.0),
    (4, 1.0),
    (null, null),
    (null, 5.0),
    (6, null);

-- count bug, empty groups should evaluate to 0
select *, (select count(*) from r where l.a = r.c) from l;

-- no count bug, empty groups should evaluate to null
select *, (select count(*) from r where l.a = r.c group by c) from l;
select *, (select count(*) from r where l.a = r.c group by 'constant') from l;

-- count bug, empty groups should evaluate to false
select *, (
  select (count(*)) is null
  from r
  where l.a = r.c)
from l;

-- no count bug, empty groups should evaluate to null
select *, (
  select (count(*)) is null
  from r
  where l.a = r.c
  group by r.c)
from l;

-- Empty groups should evaluate to 0, and groups filtered by HAVING should evaluate to NULL
select *, (select count(*) from r where l.a = r.c having count(*) <= 1) from l;

-- Empty groups are filtered by HAVING and should evaluate to null
select *, (select count(*) from r where l.a = r.c having count(*) >= 2) from l;


set spark.sql.optimizer.decorrelateSubqueryLegacyIncorrectCountHandling.enabled = true;

-- With legacy behavior flag set, both cases evaluate to 0
select *, (select count(*) from r where l.a = r.c) from l;
select *, (select count(*) from r where l.a = r.c group by c) from l;
select *, (select count(*) from r where l.a = r.c group by 'constant') from l;

reset spark.sql.optimizer.decorrelateSubqueryLegacyIncorrectCountHandling.enabled;

CREATE OR REPLACE VIEW t1(a1, a2) as values (0, 1), (1, 2);
CREATE OR REPLACE VIEW t2(b1, b2) as values (0, 2), (0, 3);
CREATE OR REPLACE VIEW t3(c1, c2) as values (0, 2), (0, 3);

-- test for count bug in correlated scalar subqueries
select ( select sum(cnt) from (select count(*) cnt from t2 where t1.a1 = t2.b1) ) a from t1 order by a desc;

-- test for count bug in correlated scalar subqueries with nested counts
select ( select count(*) from (select count(*) cnt from t2 where t1.a1 = t2.b1) ) a from t1 order by a desc;

-- test for count bug in correlated scalar subqueries with multiple count aggregates
select (
  select SUM(l.cnt + r.cnt)
  from (select count(*) cnt from t2 where t1.a1 = t2.b1 having cnt = 0) l
  join (select count(*) cnt from t3 where t1.a1 = t3.c1 having cnt = 0) r
  on l.cnt = r.cnt
) a from t1 order by a desc;

-- same as above, without HAVING clause
select (
  select sum(l.cnt + r.cnt)
  from (select count(*) cnt from t2 where t1.a1 = t2.b1) l
  join (select count(*) cnt from t3 where t1.a1 = t3.c1) r
  on l.cnt = r.cnt
) a from t1 order by a desc;

DROP VIEW t1;
DROP VIEW t2;
DROP VIEW t3;
