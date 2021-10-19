-- Test cases for lateral join

CREATE VIEW t1(c1, c2) AS VALUES (0, 1), (1, 2);
CREATE VIEW t2(c1, c2) AS VALUES (0, 2), (0, 3);

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

-- lateral join can reference preceding FROM clause items
SELECT * FROM t1 JOIN t2 JOIN LATERAL (SELECT t1.c2 + t2.c2);
-- expect error: cannot resolve `t2.c1`
SELECT * FROM t1 JOIN LATERAL (SELECT t1.c1 AS a, t2.c1 AS b) s JOIN t2 ON s.b = t2.c1;

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

-- clean up
DROP VIEW t1;
DROP VIEW t2;
