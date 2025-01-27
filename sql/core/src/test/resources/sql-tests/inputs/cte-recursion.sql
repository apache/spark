--CONFIG_DIM2 spark.sql.adaptive.enabled=false
--CONFIG_DIM2 spark.sql.adaptive.enabled=true

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

-- unlimited recursion fails at spark.sql.cte.recursion.level.limits level
WITH RECURSIVE r(level) AS (
  VALUES 0
  UNION ALL
  SELECT level + 1 FROM r
)
SELECT * FROM r;

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
SELECT * FROM t;

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


-- recursive reference is not allowed in a nested CTE
-- TABLE_OR_VIEW_NOT_FOUND is thrown now, although it some check should be added to exactly inform
-- that this is not allowed
WITH RECURSIVE
  t1 AS (
    SELECT 1 AS level
    UNION (
      WITH t2 AS (SELECT level + 1 FROM t1 WHERE level < 10)
      SELECT * FROM t2
    )
  )
SELECT * FROM t1;

-- recursive reference and conflicting outer CTEs are not allowed in a nested CTE
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
