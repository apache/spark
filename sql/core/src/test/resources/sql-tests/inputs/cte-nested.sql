-- CTE in CTE definition
WITH t as (
  WITH t2 AS (SELECT 1)
  SELECT * FROM t2
)
SELECT * FROM t;

-- CTE in subquery
SELECT max(c) FROM (
  WITH t(c) AS (SELECT 1)
  SELECT * FROM t
);

-- CTE in subquery expression
SELECT (
  WITH t AS (SELECT 1)
  SELECT * FROM t
);

-- Make sure CTE in subquery is scoped to that subquery rather than global
-- the 2nd half of the union should fail because the cte is scoped to the first half
SELECT * FROM
  (
   WITH cte AS (SELECT * FROM range(10))
   SELECT * FROM cte WHERE id = 8
  ) a
UNION
SELECT * FROM cte;

-- CTE in CTE definition shadows outer
WITH
  t AS (SELECT 1),
  t2 AS (
    WITH t AS (SELECT 2)
    SELECT * FROM t
  )
SELECT * FROM t2;

-- CTE in CTE definition shadows outer 2
WITH
  t(c) AS (SELECT 1),
  t2 AS (
    SELECT (
      SELECT max(c) FROM (
        WITH t(c) AS (SELECT 2)
        SELECT * FROM t
      )
    )
  )
SELECT * FROM t2;

-- CTE in CTE definition shadows outer 3
WITH
  t AS (SELECT 1),
  t2 AS (
    WITH t AS (SELECT 2),
    t2 AS (
      WITH t AS (SELECT 3)
      SELECT * FROM t
    )
    SELECT * FROM t2
  )
SELECT * FROM t2;

-- CTE in subquery shadows outer
WITH t(c) AS (SELECT 1)
SELECT max(c) FROM (
  WITH t(c) AS (SELECT 2)
  SELECT * FROM t
);

-- CTE in subquery shadows outer 2
WITH t(c) AS (SELECT 1)
SELECT sum(c) FROM (
  SELECT max(c) AS c FROM (
    WITH t(c) AS (SELECT 2)
    SELECT * FROM t
  )
);

-- CTE in subquery shadows outer 3
WITH t(c) AS (SELECT 1)
SELECT sum(c) FROM (
  WITH t(c) AS (SELECT 2)
  SELECT max(c) AS c FROM (
    WITH t(c) AS (SELECT 3)
    SELECT * FROM t
  )
);

-- CTE in subquery expression shadows outer
WITH t AS (SELECT 1)
SELECT (
  WITH t AS (SELECT 2)
  SELECT * FROM t
);

-- CTE in subquery expression shadows outer 2
WITH t AS (SELECT 1)
SELECT (
  SELECT (
    WITH t AS (SELECT 2)
    SELECT * FROM t
  )
);

-- CTE in subquery expression shadows outer 3
WITH t AS (SELECT 1)
SELECT (
  WITH t AS (SELECT 2)
  SELECT (
    WITH t AS (SELECT 3)
    SELECT * FROM t
  )
);

-- CTE in subquery expression shadows outer 4
WITH t(c) AS (SELECT 1)
SELECT * FROM t
WHERE c IN (
  WITH t(c) AS (SELECT 2)
  SELECT * FROM t
);

-- forward name conflict is not a real conflict
WITH
  t AS (
    WITH t2 AS (SELECT 1)
    SELECT * FROM t2
  ),
  t2 AS (SELECT 2)
SELECT * FROM t;

-- case insensitive name conflicts: in other CTE relations
WITH
  abc AS (SELECT 1),
  t AS (
    WITH aBc AS (SELECT 2)
    SELECT * FROM aBC
  )
SELECT * FROM t;

-- case insensitive name conflicts: in subquery expressions
WITH abc AS (SELECT 1)
SELECT (
  WITH aBc AS (SELECT 2)
  SELECT * FROM aBC
);

-- SPARK-38404: CTE in CTE definition references outer
WITH
  t1 AS (SELECT 1),
  t2 AS (
    WITH t3 AS (
      SELECT * FROM t1
    )
    SELECT * FROM t3
  )
SELECT * FROM t2;

-- CTE nested in CTE main body FROM clause references outer CTE def
WITH cte_outer AS (
  SELECT 1
)
SELECT * FROM (
  WITH cte_inner AS (
    SELECT * FROM cte_outer
  )
  SELECT * FROM cte_inner
);

-- CTE double nested in CTE main body FROM clause references outer CTE def
WITH cte_outer AS (
  SELECT 1
)
SELECT * FROM (
  WITH cte_inner AS (
    SELECT * FROM (
      WITH cte_inner_inner AS (
        SELECT * FROM cte_outer
      )
      SELECT * FROM cte_inner_inner
    )
  )
  SELECT * FROM cte_inner
);

-- Invalid reference to invisible CTE def nested CTE def
WITH cte_outer AS (
  WITH cte_invisible_inner AS (
    SELECT 1
  )
  SELECT * FROM cte_invisible_inner
)
SELECT * FROM (
  WITH cte_inner AS (
    SELECT * FROM cte_invisible_inner
  )
  SELECT * FROM cte_inner
);

-- Invalid reference to invisible CTE def nested CTE def (in FROM)
WITH cte_outer AS (
  SELECT * FROM (
    WITH cte_invisible_inner AS (
      SELECT 1
    )
    SELECT * FROM cte_invisible_inner
  )
)
SELECT * FROM (
  WITH cte_inner AS (
    SELECT * FROM cte_invisible_inner
  )
  SELECT * FROM cte_inner
);