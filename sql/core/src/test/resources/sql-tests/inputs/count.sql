-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (1, 1), (null, 2), (1, null), (null, null)
AS testData(a, b);

-- count with single expression
SELECT
  count(*), count(1), count(null), count(a), count(b), count(a + b), count((a, b))
FROM testData;

-- distinct count with single expression
SELECT
  count(DISTINCT 1),
  count(DISTINCT null),
  count(DISTINCT a),
  count(DISTINCT b),
  count(DISTINCT (a + b)),
  count(DISTINCT (a, b))
FROM testData;

-- count with multiple expressions
SELECT count(a, b), count(b, a), count(testData.*, testData.*) FROM testData;

-- distinct count with multiple expressions
SELECT
  count(DISTINCT a, b), count(DISTINCT b, a), count(DISTINCT *), count(DISTINCT testData.*, testData.*)
FROM testData;

-- distinct count with multiple literals
SELECT count(DISTINCT 3,2);
SELECT count(DISTINCT 2), count(DISTINCT 2,3);
SELECT count(DISTINCT 2), count(DISTINCT 3,2);
SELECT count(DISTINCT a), count(DISTINCT 2,3) FROM testData;
SELECT count(DISTINCT a), count(DISTINCT 3,2) FROM testData;
SELECT count(DISTINCT a), count(DISTINCT 2), count(DISTINCT 2,3) FROM testData;
SELECT count(DISTINCT a), count(DISTINCT 2), count(DISTINCT 3,2) FROM testData;
SELECT count(distinct 0.8), percentile_approx(distinct a, 0.8) FROM testData;

-- legacy behavior: allow calling function count without parameters
set spark.sql.legacy.allowParameterlessCount=true;
SELECT count() FROM testData;

-- count without expressions
set spark.sql.legacy.allowParameterlessCount=false;
SELECT count() FROM testData;

-- legacy behavior: allow count(testData.*)
set spark.sql.legacy.allowStarWithSingleTableIdentifierInCount=true;
SELECT count(testData.*) FROM testData;

-- count with a single tblName.* as parameter
set spark.sql.legacy.allowStarWithSingleTableIdentifierInCount=false;
SELECT count(testData.*) FROM testData;

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
