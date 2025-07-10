--SET spark.sql.cteRecursionLevelLimit=25
--SET spark.sql.cteRecursionRowLimit=50

-- fails due to recursion isn't allowed without RECURSIVE keyword
WITH r(level) AS (
  VALUES 0
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 9
)
SELECT * FROM r;

-- basic recursion
WITH RECURSIVE r AS (
  SELECT 0 AS level
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 9
)
SELECT * FROM r;

-- basic recursion with subquery column alias
WITH RECURSIVE r(level) AS (
  VALUES 0
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 9
)
SELECT * FROM r;

-- using string column in recursion
WITH RECURSIVE r(c) AS (
  SELECT 'a'
  UNION ALL
  SELECT c || char(ascii(substr(c, -1)) + 1) FROM r WHERE LENGTH(c) < 10
)
SELECT * FROM r;

-- unlimited recursion fails at spark.sql.cteRecursionLevelLimit level
WITH RECURSIVE r(level) AS (
  VALUES 0
  UNION ALL
  SELECT level + 1 FROM r
)
SELECT * FROM r;

-- unlimited recursion fails at spark.sql.cteRecursionRowLimit level
CREATE TEMPORARY VIEW ZeroAndOne(current, next) AS VALUES
    (0,0),
    (0,1),
    (1,0),
    (1,1);

WITH RECURSIVE t(n) AS (
    SELECT 1
    UNION ALL
    SELECT next FROM t LEFT JOIN ZeroAndOne ON n = current
)
SELECT * FROM t;

DROP VIEW ZeroAndOne;

-- limited recursion doesn't fail at spark.sql.cteRecursionLevelLimit by setting MAX RECURSION LEVEL
WITH RECURSIVE r(level) MAX RECURSION LEVEL 35 AS (
  VALUES 0
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 30
  )
SELECT * FROM r;

-- setting MAX RECURSION LEVEL without keyword RECURSIVE fails
WITH r(level) MAX RECURSION LEVEL 35 AS (
  VALUES 0
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 30
)
SELECT * FROM r;

-- limited recursion fails at spark.sql.cteRecursionLevelLimit level because it is too low
WITH RECURSIVE r(level) AS (
  VALUES 0
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 150
)
SELECT * FROM r;

-- unlimited recursion stopped from failing by putting LIMIT
CREATE TEMPORARY VIEW ZeroAndOne(current, next) AS VALUES
    (0,0),
    (0,1),
    (1,0),
    (1,1);

WITH RECURSIVE t(n) AS (
    SELECT 1
    UNION ALL
    SELECT next FROM t LEFT JOIN ZeroAndOne ON n = current
    )
SELECT * FROM t LIMIT 60;

DROP VIEW ZeroAndOne;

-- terminate recursion with LIMIT
WITH RECURSIVE r(level) AS (
  VALUES 0
  UNION ALL
  SELECT level + 1 FROM r
)
SELECT * FROM r LIMIT 10;

-- UNION - not yet supported
WITH RECURSIVE r AS (
  SELECT 0 as level
  UNION
  SELECT (level + 1) % 10 FROM r
)
SELECT * FROM r;

-- UNION with subquery column alias - not yet supported
WITH RECURSIVE r(level) AS (
  VALUES 0
  UNION
  SELECT (level + 1) % 10 FROM r
)
SELECT * FROM r;

-- unlimited recursion fails because using LIMIT to terminate recursion only works where it can be
-- pushed down to recursion
WITH RECURSIVE r(level) AS (
  VALUES 0
  UNION ALL
  SELECT level + 1 FROM r
)
SELECT level, level FROM r ORDER BY 1 LIMIT 10;

-- fails because recursion doesn't follow the expected form
WITH RECURSIVE r(level) AS (
  SELECT level + 1 FROM r WHERE level < 9
)
SELECT * FROM r;

-- fails because recursion doesn't follow the expected form
WITH RECURSIVE r(level) AS (
  SELECT level + 1 FROM r WHERE level < 9
  UNION ALL
  VALUES 0
)
SELECT * FROM r;

--recursive keyword but no self-reference
--other sql engines don't throw an error in this case, so Spark doesn't throw it as well
WITH RECURSIVE t(n) AS (
    SELECT 1
    UNION ALL
    SELECT 2
)
SELECT * FROM t;

-- fails because a recursive query should contain UNION ALL or UNION combinator
WITH RECURSIVE r(level) AS (
  VALUES 0
  INTERSECT
  SELECT level + 1 FROM r WHERE level < 9
)
SELECT * FROM r;

-- recursive reference is not allowed in a subquery expression
WITH RECURSIVE t(col) (
  SELECT 1
  UNION ALL
  SELECT (SELECT max(col) FROM t)
)
SELECT * FROM t LIMIT 5;

-- complicated subquery example: self-reference in subquery in an inner CTE
WITH
  t1 AS (SELECT 1 as n),
  t2(n) AS (
    WITH RECURSIVE t3(n) AS (
      SELECT 1
      UNION ALL
      SELECT n+1 FROM (SELECT MAX(n) FROM t3)
    )
    SELECT * FROM t3
  )
SELECT * FROM t2;

-- Self reference is inside OneRowSubquery
WITH RECURSIVE t1(n) AS (
    SELECT 1
    UNION ALL
    SELECT (SELECT n+1 FROM t1 WHERE n<5)
)
SELECT * FROM t1 LIMIT 5;

-- recursive reference in a nested CTE
WITH RECURSIVE
  t1 AS (
    SELECT 1 AS level
    UNION ALL (
      WITH t2 AS (SELECT level + 1 FROM t1 WHERE level < 10)
      SELECT * FROM t2
    )
  )
SELECT * FROM t1;

-- recursive reference and conflicting outer CTEs in a nested CTE
SET spark.sql.legacy.ctePrecedencePolicy=CORRECTED;
WITH
  t1 AS (SELECT 1),
  t2 AS (
    WITH RECURSIVE
      t1 AS (
        SELECT 1 AS level
        UNION ALL (
          WITH t3 AS (SELECT level + 1 FROM t1 WHERE level < 10)
          SELECT * FROM t3
        )
      )
    SELECT * FROM t1
  )
SELECT * FROM t2;
SET spark.sql.legacy.ctePrecedencePolicy=EXCEPTION;

-- recursive reference can't be used multiple times in a recursive term
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 0)
  UNION ALL
  SELECT r1.level + 1, r1.data
  FROM r AS r1
  JOIN r AS r2 ON r2.data = r1.data
  WHERE r1.level < 9
)
SELECT * FROM r;

-- recursive reference is allowed on left side of a left outer join
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 0)
  UNION ALL
  SELECT level + 1, r.data
  FROM r
  LEFT OUTER JOIN (
    SELECT 0 AS data
  ) AS t ON t.data = r.data
  WHERE r.level < 9
)
SELECT * FROM r;

-- recursive reference is not allowed on right side of a left outer join
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 0)
  UNION ALL
  SELECT level + 1, r.data
  FROM (
    SELECT 0 AS data
  ) AS t
  LEFT OUTER JOIN r ON r.data = t.data
  WHERE r.level < 9
)
SELECT * FROM r;

-- recursive reference is allowed on right side of a right outer join
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 0)
  UNION ALL
  SELECT level + 1, r.data
  FROM (
    SELECT 0 AS data
  ) AS t
  RIGHT OUTER JOIN r ON r.data = t.data
  WHERE r.level < 9
)
SELECT * FROM r;

-- recursive reference is not allowed on left side of a right outer join
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 0)
  UNION ALL
  SELECT level + 1, r.data
  FROM r
  RIGHT OUTER JOIN (
    SELECT 0 AS data
  ) AS t ON t.data = r.data
  WHERE r.level < 9
)
SELECT * FROM r;

-- recursive reference is not allowed in a full outer join
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 0)
  UNION ALL
  SELECT level + 1, r.data
  FROM r
  FULL OUTER JOIN (
    SELECT 0 AS data
  ) AS t ON t.data = r.data
  WHERE r.level < 9
)
SELECT * FROM r;

-- recursive reference is allowed on left side of a left semi join
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 0)
  UNION ALL
  SELECT level + 1, r.data
  FROM r
  LEFT SEMI JOIN (
    SELECT 0 AS data
  ) AS t ON t.data = r.data
  WHERE r.level < 9
)
SELECT * FROM r;

-- recursive reference is not allowed on right side of a left semi join
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 0)
  UNION ALL
  SELECT level + 1, data
  FROM (
    SELECT 0 AS level, 0 AS data
  ) AS t
  LEFT SEMI JOIN r ON r.data = t.data AND r.level < 9
)
SELECT * FROM r;

-- recursive reference is allowed on left side of a left anti join
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 0)
  UNION ALL
  SELECT level + 1, r.data
  FROM r
  LEFT ANTI JOIN (
    SELECT -1 AS data
  ) AS t ON t.data = r.data
  WHERE r.level < 9
)
SELECT * FROM r;

-- recursive reference is not allowed on right side of a left anti join
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 0)
  UNION ALL
  SELECT level + 1, data
  FROM (
    SELECT 0 AS level, 0 AS data
  ) AS t
  LEFT ANTI JOIN r ON r.data = t.data AND r.level < 9
)
SELECT * FROM r;

-- recursive reference is not allowed in an aggregate
WITH RECURSIVE r(level, data) AS (
  VALUES (0, 1L)
  UNION ALL
  SELECT MAX(level) + 1, SUM(data) FROM r WHERE level < 9
)
SELECT * FROM r;

-- group by inside recursion not allowed (considered as aggregate as well)
WITH RECURSIVE r(n) AS (
    SELECT 1
    UNION ALL
    SELECT n+1 FROM r GROUP BY n)
SELECT * FROM r;

-- recursion is allowed in simple commands
CREATE TEMPORARY VIEW rv AS
WITH RECURSIVE r(level) AS (
  VALUES (0)
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 9
)
SELECT * FROM r;

SELECT * FROM rv;

DROP VIEW rv;

-- recursion is allowed in simple commands 2
CREATE TABLE rt(level INT) USING csv;

WITH RECURSIVE r(level) AS (
  VALUES (0)
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 9
)
INSERT INTO rt SELECT * FROM r;

SELECT * from rt;

DROP TABLE rt;

-- recursion is not allowed in multi commands
CREATE TABLE rt2(level INT) USING csv;

WITH RECURSIVE r(level) AS (
    VALUES (0)
    UNION ALL
    SELECT level + 1 FROM r WHERE level < 9
)
FROM r
INSERT INTO rt2 SELECT *
INSERT INTO rt2 SELECT *;

SELECT * FROM rt2;

DROP TABLE rt2;

-- multiple recursive CTEs
WITH RECURSIVE
  r1 AS (
    SELECT 0 AS level
    UNION ALL
    SELECT level + 1 FROM r1 WHERE level < 9
  ),
  r2 AS (
    SELECT 10 AS level
    UNION ALL
    SELECT level + 1 FROM r2 WHERE level < 19
  )
SELECT *
FROM r1
JOIN r2 ON r2.level = r1.level + 10;

-- multiple uses of recursive CTEs
WITH RECURSIVE r AS (
  SELECT 0 AS level
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 9
)
SELECT *
FROM r AS r1
JOIN r AS r2 ON r2.level = r1.level;

-- recursive cte nested into recursive cte as anchor
WITH RECURSIVE r2 AS (
  WITH RECURSIVE r1 AS (
    SELECT 0 AS innerlevel
    UNION ALL
    SELECT innerlevel + 1 FROM r1 WHERE innerlevel < 3
  )
  SELECT 0 AS outerlevel, innerlevel FROM r1
  UNION ALL
  SELECT outerlevel + 1, innerlevel FROM r2 WHERE outerlevel < 3
)
SELECT * FROM r2;

-- name collision of nested CTEs (the outer CTE is not recursive)
WITH RECURSIVE r(level) AS (
  WITH RECURSIVE r(level) AS (
    VALUES 0
    UNION ALL
    SELECT level + 1 FROM r WHERE level < 3
  )
  SELECT * FROM r
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 3
)
SELECT * FROM r;

-- name collision of nested CTEs (the outer CTE is recursive)
WITH RECURSIVE r(level) AS (
  (WITH RECURSIVE r(level) AS (
    VALUES 0
    UNION ALL
    SELECT level + 1 FROM r WHERE level < 3
  )
  SELECT * FROM r)
  UNION ALL
  SELECT level + 1 FROM r WHERE level < 3
)
SELECT * FROM r;

-- routes represented here is as follows:
--
-- New York<--->Boston
-- |            ∧
-- ∨            |
-- Washington---+
-- |
-- ∨
-- Raleigh
CREATE TEMPORARY VIEW routes(origin, destination) AS VALUES
  ('New York', 'Washington'),
  ('New York', 'Boston'),
  ('Boston', 'New York'),
  ('Washington', 'Boston'),
  ('Washington', 'Raleigh');

-- handling cycles that could cause infinite recursion
WITH RECURSIVE destinations_from_new_york AS (
  SELECT 'New York' AS destination, ARRAY('New York') AS path, 0 AS length
  UNION ALL
  SELECT r.destination, CONCAT(d.path, ARRAY(r.destination)), d.length + 1
  FROM routes AS r
  JOIN destinations_from_new_york AS d ON d.destination = r.origin AND NOT ARRAY_CONTAINS(d.path, r.destination)
)
SELECT * FROM destinations_from_new_york;

DROP VIEW routes;

-- Fibonacci numbers
WITH RECURSIVE fibonacci AS (
  VALUES (0, 1) AS t(a, b)
  UNION ALL
  SELECT b, a + b FROM fibonacci WHERE a < 10
)
SELECT a FROM fibonacci ORDER BY a;

-- Recursive CTE with useless columns
WITH RECURSIVE t1(a,b,c) AS (
    SELECT 1,1,1
    UNION ALL
    SELECT a+1,a+1,a+1 FROM t1)
SELECT a FROM t1 LIMIT 5;

-- CROSS JOIN example
CREATE TABLE tb (next INT) USING json;

INSERT INTO tb VALUES (0), (1);

WITH RECURSIVE t(n) AS (
    SELECT 1
    UNION ALL
    SELECT next FROM t CROSS JOIN tb
    )
SELECT * FROM t LIMIT 63;

DROP TABLE tb;
-- CROSS JOIN example 2

WITH RECURSIVE
    x(id) AS (SELECT 1 UNION SELECT 2),
    t(id, xid) AS (
        SELECT 0 AS id, 0 AS xid
        UNION ALL
        SELECT t.id + 1, xid * 10 + x.id FROM t CROSS JOIN x WHERE t.id < 3
    )
SELECT * FROM t;

-- rCTE referencing other rCTE
WITH RECURSIVE t1(a, b) AS (
    SELECT 1, 1
    UNION ALL
    SELECT a + b, a FROM t1 WHERE a < 20
),
t2(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM t2, t1 WHERE n + 1 = a
)
SELECT * FROM t2;

-- Multiple CTEs within rCTE
WITH RECURSIVE t1 (n) AS (
    VALUES(1)
    UNION ALL
    (
    WITH t2(j) AS (
            SELECT n + 1 FROM t1
        ),
        t3(k) AS (
            SELECT j FROM t2
        )
        SELECT k FROM t3 WHERE k <= 5
    )
)
SELECT n FROM t1;

-- Column aliases with CTEs inside rCTEs
WITH RECURSIVE r2(outerlevel1, innerlevel1) AS (
    WITH RECURSIVE r1 AS (
        SELECT 0 AS innerlevel
        UNION ALL
        SELECT innerlevel + 1 FROM r1 WHERE innerlevel < 3
    )
    SELECT 0 AS outerlevel, innerlevel FROM r1
    UNION ALL
    SELECT outerlevel1 + 1, innerlevel1 FROM r2 WHERE outerlevel1 < 3
)
SELECT * FROM r2;

-- An inner cte that is defined for both the anchor and recursion but called only in the recursion
-- with subquery alias
WITH RECURSIVE t1(n) AS (
    WITH t2(n) AS (SELECT * FROM t1)
    SELECT 1
    UNION ALL
    SELECT n+1 FROM t2 WHERE n < 5)
SELECT * FROM t1;

-- An inner cte that is defined for both the anchor and recursion but called only in the recursion
-- without query alias
WITH RECURSIVE t1 AS (
    WITH t2(n) AS (SELECT * FROM t1)
    SELECT 1 AS n
    UNION ALL
    SELECT n+1 FROM t2 WHERE n < 5)
SELECT * FROM t1;

-- Recursive CTE with multiple of the same reference in the anchor, which get referenced differently subsequent iterations.
WITH RECURSIVE tmp(x) AS (
    values (1), (2), (3), (4), (5)
), rcte(x, y) AS (
    SELECT x, x FROM tmp WHERE x = 1
    UNION ALL
    SELECT x + 1, x FROM rcte WHERE x < 5
)
SELECT * FROM rcte;

-- Previous query without converting to local relations
SET spark.sql.cteRecursionAnchorRowsLimitToConvertToLocalRelation=0;

WITH RECURSIVE tmp(x) AS (
    values (1), (2), (3), (4), (5)
), rcte(x, y) AS (
    SELECT x, x FROM tmp WHERE x = 1
    UNION ALL
    SELECT x + 1, x FROM rcte WHERE x < 5
)
SELECT * FROM rcte;

SET spark.sql.cteRecursionAnchorRowsLimitToConvertToLocalRelation=100;

-- Recursive CTE with multiple of the same reference in the anchor, which get referenced as different variables in subsequent iterations.
WITH RECURSIVE tmp(x) AS (
    values (1), (2), (3), (4), (5)
), rcte(x, y, z, t) AS (
    SELECT x, x, x, x FROM tmp WHERE x = 1
    UNION ALL
    SELECT x + 1, x, y + 1, y FROM rcte WHERE x < 5
)
SELECT * FROM rcte;

-- Non-deterministic query with rand with seed
WITH RECURSIVE randoms(val) AS (
    SELECT CAST(floor(rand(82374) * 5 + 1) AS INT)
    UNION ALL
    SELECT CAST(floor(rand(237685) * 5 + 1) AS INT)
    FROM randoms
)
SELECT val FROM randoms LIMIT 5;

-- Non-deterministic query with uniform with seed
WITH RECURSIVE randoms(val) AS (
    SELECT CAST(UNIFORM(1, 6, 82374) AS INT)
    UNION ALL
    SELECT CAST(UNIFORM(1, 6, 237685) AS INT)
    FROM randoms
)
SELECT val FROM randoms LIMIT 5;

-- Non-deterministic query with randn with seed
WITH RECURSIVE randoms(val) AS (
    SELECT CAST(floor(randn(82374) * 5 + 1) AS INT)
    UNION ALL
    SELECT CAST(floor(randn(237685) * 5 + 1) AS INT)
    FROM randoms
)
SELECT val FROM randoms LIMIT 5;

-- Non-deterministic query with randstr
WITH RECURSIVE randoms(val) AS (
    SELECT randstr(10, 82374)
    UNION ALL
    SELECT randstr(10, 237685)
    FROM randoms
)
SELECT val FROM randoms LIMIT 5;

-- Non-deterministic query with UUID
WITH RECURSIVE randoms(val) AS (
    SELECT UUID(82374)
    UNION ALL
    SELECT UUID(237685)
    FROM randoms
)
SELECT val FROM randoms LIMIT 5;

-- Non-deterministic query with shuffle
WITH RECURSIVE randoms(val) AS (
    SELECT ARRAY(1,2,3,4,5)
    UNION ALL
    SELECT SHUFFLE(ARRAY(1,2,3,4,5), 237685)
    FROM randoms
)
SELECT val FROM randoms LIMIT 5;

-- Type coercion where the anchor is wider
WITH RECURSIVE t1(n, m) AS (
    SELECT 1, CAST(1 AS BIGINT)
    UNION ALL
    SELECT n+1, n+1 FROM t1 WHERE n < 5)
SELECT * FROM t1;

-- Type coercion where the recursion is wider
WITH RECURSIVE t1(n, m) AS (
    SELECT 1, 1
    UNION ALL
    SELECT n+1, CAST(n+1 AS BIGINT) FROM t1 WHERE n < 5)
SELECT * FROM t1;

-- Recursive CTE with nullable recursion and non-recursive anchor
WITH RECURSIVE t1(n) AS (
    SELECT 1
    UNION ALL
    SELECT CASE WHEN n < 5 THEN n + 1 ELSE NULL END FROM t1
)
SELECT * FROM t1 LIMIT 25;

-- Two calls to same rCTE with and without limit
WITH RECURSIVE t1(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM t1 WHERE n < 5
)
SELECT (SELECT SUM(n) FROM (SELECT * FROM t1)), (SELECT SUM(n) FROM (SELECT * FROM t1 LIMIT 3));

-- Two calls to same infinite rCTE with different limits
WITH RECURSIVE t1(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM t1
)
SELECT (SELECT SUM(n) FROM (SELECT * FROM t1 LIMIT 5)), (SELECT SUM(n) FROM (SELECT * FROM t1 LIMIT 3));

-- Two calls to same infinite rCTE from another rCTE
WITH RECURSIVE t1(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM t1
), t2(m) AS (
    SELECT (SELECT SUM(n) FROM (SELECT n FROM t1 LIMIT 10) AS sums)
    UNION ALL
    SELECT m + (SELECT SUM(n) FROM (SELECT n FROM t1 LIMIT 3) AS sums) FROM t2
)
SELECT * FROM t2 LIMIT 20;

-- Two calls to recursive CTE with single limit pushed to both
WITH RECURSIVE t1(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM t1
)
    ((SELECT n FROM t1) UNION ALL (SELECT n FROM t1)) LIMIT 20;

-- Recursive CTE with self reference inside Window function
WITH RECURSIVE win(id, val) AS (
    SELECT 1, CAST(10 AS BIGINT)
    UNION ALL
    SELECT id + 1, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
    FROM win WHERE id < 3
)
SELECT * FROM win;

WITH RECURSIVE t1(n) AS (
    SELECT 1
    UNION ALL
    (SELECT n + 1 FROM t1 WHERE n < 5 ORDER BY n)
)
SELECT * FROM t1;
