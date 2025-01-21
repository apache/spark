-- Test cases for lateral join

CREATE VIEW t1(c1, c2) AS VALUES (0, 1), (1, 2);
CREATE VIEW t2(c1, c2) AS VALUES (0, 2), (0, 3);
CREATE VIEW t3(c1, c2) AS VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4));
CREATE VIEW t4(c1, c2) AS VALUES (0, 1), (0, 2), (1, 1), (1, 3);

-- lateral join with single column select
SELECT * FROM t1, LATERAL (SELECT c1);
SELECT * FROM t1, LATERAL (SELECT c1 FROM t2);
SELECT * FROM t1, LATERAL (SELECT t1.c1 FROM t2);
SELECT * FROM t1, LATERAL (SELECT t1.c1 + t2.c1 FROM t2);

-- lateral join with star expansion
SELECT * FROM t1, LATERAL (SELECT *);
SELECT * FROM t1, LATERAL (SELECT * FROM t2);
SELECT * FROM t1, LATERAL (SELECT t1.*);
SELECT * FROM t1, LATERAL (SELECT t1.*, t2.* FROM t2);
SELECT * FROM t1, LATERAL (SELECT t1.* FROM t2 AS t1);
-- expect error: cannot resolve 't1.*'
-- TODO: Currently we don't allow deep correlation so t1.* cannot be resolved using the outermost query.
SELECT * FROM t1, LATERAL (SELECT t1.*, t2.* FROM t2, LATERAL (SELECT t1.*, t2.*, t3.* FROM t2 AS t3));

-- lateral join with different join types
SELECT * FROM t1 JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3;
SELECT * FROM t1 LEFT JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3;
SELECT * FROM t1 CROSS JOIN LATERAL (SELECT c1 + c2 AS c3);
SELECT * FROM t1 NATURAL JOIN LATERAL (SELECT c1 + c2 AS c2);
SELECT * FROM t1 JOIN LATERAL (SELECT c1 + c2 AS c2) USING (c2);

-- lateral join without outer column references
SELECT * FROM LATERAL (SELECT * FROM t1);
SELECT * FROM t1, LATERAL (SELECT * FROM t2);
SELECT * FROM LATERAL (SELECT * FROM t1), LATERAL (SELECT * FROM t2);
SELECT * FROM LATERAL (SELECT * FROM t1) JOIN LATERAL (SELECT * FROM t2);

-- lateral join with subquery alias
SELECT a, b FROM t1, LATERAL (SELECT c1, c2) s(a, b);

-- lateral join with foldable outer query references
SELECT * FROM (SELECT 1 AS c1, 2 AS c2), LATERAL (SELECT c1, c2);

-- lateral join with correlated equality predicates
SELECT * FROM t1, LATERAL (SELECT c2 FROM t2 WHERE t1.c1 = t2.c1);

-- lateral join with correlated non-equality predicates
SELECT * FROM t1, LATERAL (SELECT c2 FROM t2 WHERE t1.c2 < t2.c2);

-- SPARK-36114: lateral join with aggregation and correlated non-equality predicates
SELECT * FROM t1, LATERAL (SELECT max(c2) AS m FROM t2 WHERE t1.c2 < t2.c2);

-- lateral join can reference preceding FROM clause items
SELECT * FROM t1 JOIN t2 JOIN LATERAL (SELECT t1.c2 + t2.c2);
-- expect error: cannot resolve `t2.c1`
SELECT * FROM t1 JOIN LATERAL (SELECT t1.c1 AS a, t2.c1 AS b) s JOIN t2 ON s.b = t2.c1;

-- SPARK-37716: lateral join with non-deterministic expressions.
-- non-deterministic lateral subquery with single row relation.
SELECT x FROM VALUES (0) t(x) JOIN LATERAL (SELECT x + rand(0) AS y);
SELECT x FROM (SELECT SUM(c1) AS x FROM t1), LATERAL (SELECT x + rand(0) AS y);
-- expect error: lateral subquery must be deterministic when joining with a multi-row relation.
SELECT * FROM t1, LATERAL (SELECT c1 + c2 + rand(0) AS c3);
SELECT * FROM t1, LATERAL (SELECT rand(0) FROM t2);
-- expect error: lateral join cannot have non-deterministic join condition.
SELECT * FROM t1 JOIN LATERAL (SELECT * FROM t2) s ON t1.c1 + rand(0) = s.c1;

-- multiple lateral joins
SELECT * FROM t1,
LATERAL (SELECT c1 + c2 AS a),
LATERAL (SELECT c1 - c2 AS b),
LATERAL (SELECT a * b AS c);

-- lateral join in between regular joins
SELECT * FROM t1
LEFT OUTER JOIN LATERAL (SELECT c2 FROM t2 WHERE t1.c1 = t2.c1) s
LEFT OUTER JOIN t1 t3 ON s.c2 = t3.c2;

-- nested lateral joins
SELECT * FROM t1, LATERAL (SELECT * FROM t2, LATERAL (SELECT c1));
SELECT * FROM t1, LATERAL (SELECT * FROM (SELECT c1 + 1 AS c1), LATERAL (SELECT c1));
SELECT * FROM t1, LATERAL (
  SELECT * FROM (SELECT c1, MIN(c2) m FROM t2 WHERE t1.c1 = t2.c1 GROUP BY c1) s,
  LATERAL (SELECT m WHERE m > c1)
);
-- expect error: cannot resolve `t1.c1`
SELECT * FROM t1, LATERAL (SELECT * FROM t2, LATERAL (SELECT t1.c1 + t2.c1));
-- expect error: cannot resolve `c2`
SELECT * FROM t1, LATERAL (SELECT * FROM (SELECT c1), LATERAL (SELECT c2));

-- uncorrelated scalar subquery inside lateral join
SELECT * FROM t1, LATERAL (SELECT c2, (SELECT MIN(c2) FROM t2));

-- correlated scalar subquery inside lateral join
SELECT * FROM t1, LATERAL (SELECT (SELECT SUM(c2) FROM t2 WHERE c1 = a) FROM (SELECT c1 AS a));
-- expect error: cannot resolve `t1.c1`
SELECT * FROM t1, LATERAL (SELECT c1, (SELECT SUM(c2) FROM t2 WHERE c1 = t1.c1));

-- lateral join inside uncorrelated subquery
SELECT * FROM t1 WHERE c1 = (SELECT MIN(a) FROM t2, LATERAL (SELECT c1 AS a));

-- lateral join inside correlated subquery
SELECT * FROM t1 WHERE c1 = (SELECT MIN(a) FROM t2, LATERAL (SELECT c1 AS a) WHERE c1 = t1.c1);

-- join condition has a correlated reference to the left side of the lateral join
SELECT * FROM t1 JOIN lateral (SELECT * FROM t2 JOIN t4 ON t2.c1 = t4.c1 AND t2.c1 = t1.c1);
SELECT * FROM t1 JOIN lateral (SELECT * FROM t2 JOIN t4 ON t2.c1 != t4.c1 AND t2.c1 != t1.c1);
SELECT * FROM t1 LEFT JOIN lateral (SELECT * FROM t4 LEFT JOIN t2 ON t2.c1 = t4.c1 AND t2.c1 = t1.c1);

-- COUNT bug with a single aggregate expression
SELECT * FROM t1, LATERAL (SELECT COUNT(*) cnt FROM t2 WHERE c1 = t1.c1);

-- COUNT bug with multiple aggregate expressions
SELECT * FROM t1, LATERAL (SELECT COUNT(*) cnt, SUM(c2) sum FROM t2 WHERE c1 = t1.c1);

-- COUNT bug without count aggregate
SELECT * FROM t1, LATERAL (SELECT SUM(c2) IS NULL FROM t2 WHERE t1.c1 = t2.c1);

-- COUNT bug with complex aggregate expressions
SELECT * FROM t1, LATERAL (SELECT COUNT(*) + CASE WHEN sum(c2) IS NULL THEN 0 ELSE sum(c2) END FROM t2 WHERE t1.c1 = t2.c1);

-- COUNT bug with non-empty group by columns (should not handle the count bug)
SELECT * FROM t1, LATERAL (SELECT c1, COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1 GROUP BY c1);
SELECT * FROM t1, LATERAL (SELECT c2, COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1 GROUP BY c2);

-- COUNT bug with different join types
SELECT * FROM t1 JOIN LATERAL (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1);
SELECT * FROM t1 LEFT JOIN LATERAL (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1);
SELECT * FROM t1 CROSS JOIN LATERAL (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1);

-- COUNT bug with group by columns and different join types
SELECT * FROM t1 LEFT JOIN LATERAL (SELECT c1, COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1 GROUP BY c1);
SELECT * FROM t1 CROSS JOIN LATERAL (SELECT c1, COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1 GROUP BY c1);

-- COUNT bug with non-empty join conditions
SELECT * FROM t1 JOIN LATERAL (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1) ON cnt + 1 = c1;

-- COUNT bug with self join
SELECT * FROM t1, LATERAL (SELECT COUNT(*) cnt FROM t1 t2 WHERE t1.c1 = t2.c1);
SELECT * FROM t1, LATERAL (SELECT COUNT(*) cnt FROM t1 t2 WHERE t1.c1 = t2.c1 HAVING cnt > 0);

-- COUNT bug with multiple aggregates
SELECT * FROM t1, LATERAL (SELECT SUM(cnt) FROM (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1));
SELECT * FROM t1, LATERAL (SELECT COUNT(cnt) FROM (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1));
SELECT * FROM t1, LATERAL (SELECT SUM(cnt) FROM (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1 GROUP BY c1));
SELECT * FROM t1, LATERAL (
  SELECT COUNT(*) FROM (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1)
  JOIN t2 ON cnt = t2.c1
);

-- COUNT bug with correlated predicates above the left outer join
SELECT * FROM t1, LATERAL (SELECT * FROM (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1) WHERE cnt = c1 - 1);
SELECT * FROM t1, LATERAL (SELECT COUNT(*) FROM (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1) WHERE cnt = c1 - 1);
SELECT * FROM t1, LATERAL (
  SELECT COUNT(*) FROM (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1)
  WHERE cnt = c1 - 1 GROUP BY cnt
);

-- COUNT bug with joins in the subquery
SELECT * FROM t1, LATERAL (
  SELECT * FROM (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1)
  JOIN t2 ON cnt = t2.c1
);
SELECT * FROM t1, LATERAL (
  SELECT l.cnt + r.cnt
  FROM (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1) l
  JOIN (SELECT COUNT(*) cnt FROM t2 WHERE t1.c1 = t2.c1) r
);

-- lateral subquery with group by
SELECT * FROM t1 LEFT JOIN LATERAL (SELECT MIN(c2) FROM t2 WHERE c1 = t1.c1 GROUP BY c1);

-- lateral join inside CTE
WITH cte1 AS (
  SELECT c1 FROM t1
), cte2 AS (
  SELECT s.* FROM cte1, LATERAL (SELECT * FROM t2 WHERE c1 = cte1.c1) s
)
SELECT * FROM cte2;

-- SPARK-41441: lateral join with outer references in Generate
SELECT * FROM t3 JOIN LATERAL (SELECT EXPLODE(c2));
SELECT * FROM t3 JOIN LATERAL (SELECT EXPLODE_OUTER(c2));
SELECT * FROM t3 JOIN LATERAL (SELECT EXPLODE(c2)) t(c3) ON c1 = c3;
SELECT * FROM t3 LEFT JOIN LATERAL (SELECT EXPLODE(c2)) t(c3) ON c1 = c3;

-- Window func
SELECT * FROM t1 JOIN LATERAL
  (SELECT sum(t2.c2) over (order by t2.c1)
  FROM   t2
  WHERE  t2.c1 >= t1.c1);

-- lateral join with union
SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  UNION ALL
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 = t1.c1);

SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  UNION DISTINCT
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 > t1.c2);

-- INTERSECT
SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  INTERSECT ALL
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 = t1.c1);

SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  INTERSECT DISTINCT
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 > t1.c2);

-- EXCEPT
SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  EXCEPT ALL
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 = t1.c1);

SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  EXCEPT DISTINCT
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 > t1.c2);

-- COUNT bug with UNION in subquery
SELECT * FROM t1 JOIN LATERAL
  (SELECT COUNT(t2.c2)
  FROM   t2
  WHERE  t2.c1 = t1.c1
  UNION DISTINCT
  SELECT COUNT(t4.c2)
  FROM   t4
  WHERE  t4.c1 > t1.c2);

-- Both correlated and uncorrelated children
SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c1, t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  UNION ALL
  SELECT t4.c2, t4.c1
  FROM   t4
  WHERE  t4.c1 = t1.c1);

SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1 and t2.c2 >= t1.c2
  UNION ALL
  SELECT t4.c2
  FROM   t4);

SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  UNION ALL
  SELECT t4.c2
  FROM   t4);

SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1 and t2.c2 >= t1.c2
  UNION DISTINCT
  SELECT t4.c2
  FROM   t4);

SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1 and t2.c2 >= t1.c2
  INTERSECT ALL
  SELECT t4.c2
  FROM   t4);
  
SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1 and t2.c2 >= t1.c2
  INTERSECT DISTINCT
  SELECT t4.c2
  FROM   t4);

SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1 and t2.c2 >= t1.c2
  EXCEPT ALL
  SELECT t4.c2
  FROM   t4);
  
SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1 and t2.c2 >= t1.c2
  EXCEPT DISTINCT
  SELECT t4.c2
  FROM   t4);

-- Correlation under group by
SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  GROUP BY t2.c2
  UNION ALL
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 > t1.c2
  GROUP BY t4.c2);

-- Correlation in group by
SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c1 - t1.c1
  FROM   t2
  GROUP BY t2.c1 - t1.c1
  UNION ALL
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 > t1.c2
  GROUP BY t4.c2);

-- Window func - unsupported
SELECT * FROM t1 JOIN LATERAL
  (SELECT sum(t2.c2) over (order by t2.c1)
  FROM   t2
  WHERE  t2.c1 >= t1.c1
  UNION ALL
  SELECT t4.c2
  FROM   t4);

-- lateral join under union
SELECT * FROM t1 JOIN LATERAL (SELECT * FROM t2 WHERE t2.c1 = t1.c1)
UNION ALL
SELECT * FROM t1 JOIN t4;

-- union above and below lateral join
SELECT * FROM t1 JOIN LATERAL
  (SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  UNION ALL
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 = t1.c1)
UNION ALL
SELECT * FROM t2 JOIN LATERAL
  (SELECT t1.c2
  FROM   t1
  WHERE  t2.c1 <= t1.c1
  UNION ALL
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 < t2.c1);

-- Combinations of set ops
SELECT * FROM t1 JOIN LATERAL
  ((SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  EXCEPT DISTINCT
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 > t1.c2)
  UNION DISTINCT
  (SELECT t4.c1
  FROM   t4
  WHERE  t4.c1 <= t1.c2
  INTERSECT ALL
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 <> t1.c1)
);

SELECT * FROM t1 JOIN LATERAL
  ((SELECT t2.c2
  FROM   t2
  WHERE  t2.c1 = t1.c1
  UNION ALL
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 > t1.c2)
  INTERSECT DISTINCT
  (SELECT t4.c1
  FROM   t4
  WHERE  t4.c1 <= t1.c2
  EXCEPT ALL
  SELECT t4.c2
  FROM   t4
  WHERE  t4.c1 <> t1.c1)
);

-- Semi join with correlation on left side - supported
SELECT * FROM t1 JOIN LATERAL (SELECT sum(c1) FROM
  (SELECT *
  FROM   t2
  WHERE  t2.c1 <= t1.c1) lhs
  LEFT SEMI JOIN
  (SELECT *
  FROM   t4) rhs
  ON lhs.c1 <=> rhs.c1 and lhs.c2 <=> rhs.c2
);

-- Semi join with correlation on right side - unsupported
SELECT * FROM t1 JOIN LATERAL (SELECT sum(c1) FROM
  (SELECT *
  FROM   t2
  WHERE  t2.c1 <= t1.c1) lhs
  LEFT SEMI JOIN
  (SELECT *
  FROM   t4
  WHERE t4.c1 > t1.c2) rhs
  ON lhs.c1 <=> rhs.c1 and lhs.c2 <=> rhs.c2
);

-- SPARK-41961: lateral join with table-valued functions
SELECT * FROM LATERAL EXPLODE(ARRAY(1, 2));
SELECT * FROM t1, LATERAL RANGE(3);
SELECT * FROM t1, LATERAL EXPLODE(ARRAY(c1, c2)) t2(c3);
SELECT * FROM t3, LATERAL EXPLODE(c2) t2(v);
SELECT * FROM t3, LATERAL EXPLODE_OUTER(c2) t2(v);
SELECT * FROM EXPLODE(ARRAY(1, 2)) t(v), LATERAL (SELECT v + 1);

-- lateral join with table-valued functions and join conditions
SELECT * FROM t1 JOIN LATERAL EXPLODE(ARRAY(c1, c2)) t(c3) ON t1.c1 = c3;
SELECT * FROM t3 JOIN LATERAL EXPLODE(c2) t(c3) ON t3.c1 = c3;
SELECT * FROM t3 LEFT JOIN LATERAL EXPLODE(c2) t(c3) ON t3.c1 = c3;

-- lateral join with table-valued functions in lateral subqueries
SELECT * FROM t1, LATERAL (SELECT * FROM EXPLODE(ARRAY(c1, c2)));
SELECT * FROM t1, LATERAL (SELECT t1.c1 + c3 FROM EXPLODE(ARRAY(c1, c2)) t(c3));
SELECT * FROM t1, LATERAL (SELECT t1.c1 + c3 FROM EXPLODE(ARRAY(c1, c2)) t(c3) WHERE t1.c2 > 1);
SELECT * FROM t1, LATERAL (SELECT * FROM EXPLODE(ARRAY(c1, c2)) l(x) JOIN EXPLODE(ARRAY(c2, c1)) r(y) ON x = y);

-- SPARK-42119: lateral join with table-valued functions inline and inline_outer;
CREATE OR REPLACE TEMPORARY VIEW array_struct(id, arr) AS VALUES
    (1, ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b'))),
    (2, ARRAY()),
    (3, ARRAY(STRUCT(3, 'c')));
SELECT * FROM t1, LATERAL INLINE(ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b')));
SELECT c1, t.* FROM t1, LATERAL INLINE(ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b'))) t(x, y);
SELECT * FROM array_struct JOIN LATERAL INLINE(arr);
SELECT * FROM array_struct LEFT JOIN LATERAL INLINE(arr) t(k, v) ON id = k;
SELECT * FROM array_struct JOIN LATERAL INLINE_OUTER(arr);
DROP VIEW array_struct;

-- lateral join with table-valued functions posexplode and posexplode_outer
SELECT * FROM LATERAL posexplode(ARRAY(1, 2));
SELECT * FROM t1, LATERAL posexplode(ARRAY(c1, c2)) t2(pos, c3);
SELECT * FROM t1 JOIN LATERAL posexplode(ARRAY(c1, c2)) t(pos, c3) ON t1.c1 = c3;
SELECT * FROM t3, LATERAL posexplode(c2) t2(pos, v);
SELECT * FROM t3 JOIN LATERAL posexplode(c2) t(pos, c3) ON t3.c1 = c3;
SELECT * FROM t3, LATERAL posexplode_outer(c2) t2(pos, v);
SELECT * FROM t3 LEFT JOIN LATERAL posexplode(c2) t(pos, c3) ON t3.c1 = c3;
SELECT * FROM t3 LEFT JOIN LATERAL posexplode_outer(c2) t(pos, c3) ON t3.c1 = c3;

-- lateral join with table-valued function json_tuple
CREATE OR REPLACE TEMP VIEW json_table(key, jstring) AS VALUES
    ('1', '{"f1": "1", "f2": "2", "f3": 3, "f5": 5.23}'),
    ('2', '{"f1": "1", "f3": "3", "f2": 2, "f4": 4.01}'),
    ('3', '{"f1": 3, "f4": "4", "f3": "3", "f2": 2, "f5": 5.01}'),
    ('4', cast(null as string)),
    ('5', '{"f1": null, "f5": ""}'),
    ('6', '[invalid JSON string]');
SELECT t1.key, t2.* FROM json_table t1, LATERAL json_tuple(t1.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') t2;
SELECT t1.key, t2.* FROM json_table t1, LATERAL json_tuple(t1.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') t2 WHERE t2.c0 IS NOT NULL;
SELECT t1.key, t2.* FROM json_table t1
  JOIN LATERAL json_tuple(t1.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') t2(f1, f2, f3, f4, f5)
  ON t1.key = t2.f1;
SELECT t1.key, t2.* FROM json_table t1
  LEFT JOIN LATERAL json_tuple(t1.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') t2(f1, f2, f3, f4, f5)
  ON t1.key = t2.f1;
DROP VIEW json_table;

-- lateral join with table-valued function stack
SELECT t.* FROM t1, LATERAL stack(2, 'Key', c1, 'Value', c2) t;
SELECT t.* FROM t1 JOIN LATERAL stack(1, c1, c2) t(x, y);
SELECT t.* FROM t1 JOIN t3 ON t1.c1 = t3.c1 JOIN LATERAL stack(1, t1.c2, t3.c2) t;
-- expect error
SELECT t.* FROM t1, LATERAL stack(c1, c2);

-- SPARK-36191: ORDER BY/LIMIT in the correlated subquery
select * from t1 join lateral (select * from t2 where t1.c1 = t2.c1 and t1.c2 < t2.c2 limit 1);

select * from t1 join lateral (select * from t4 where t1.c1 <= t4.c1 order by t4.c2 limit 10);

select * from t1 join lateral (select c1, min(c2) as m
                               from t2 where t1.c1 = t2.c1 and t1.c2 < t2.c2
                               group by t2.c1
                               order by m);

select * from t1 join lateral (select c1, min(c2) as m
                               from t4 where t1.c1 = t4.c1
                               group by t4.c1
                               limit 1);

select * from t1 join lateral
  ((select t4.c2 from t4 where t1.c1 <= t4.c1 order by t4.c2 limit 1)
   union all
   (select t4.c1 from t4 where t1.c1 = t4.c1 order by t4.c1 limit 3));

select * from t1 join lateral
  (select * from
   ((select t4.c2 as t from t4 where t1.c1 <= t4.c1)
   union all
   (select t4.c1 as t from t4 where t1.c1 = t4.c1)) as foo
   order by foo.t limit 5);


select 1
from t1 as t_outer
left join
 lateral(
     select b1,b2
     from
     (
         select
             t2.c1 as b1,
             1 as b2
         from t2
         union
         select t_outer.c1 as b1,
                null as b2
     ) as t_inner
     where (t_inner.b1 < t_outer.c2  or t_inner.b1 is null)
      and  t_inner.b1 = t_outer.c1
     order by t_inner.b1,t_inner.b2 desc limit 1
 ) as lateral_table;

-- clean up
DROP VIEW t1;
DROP VIEW t2;
DROP VIEW t3;
DROP VIEW t4;
