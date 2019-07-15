create temporary view t as select * from values 0, 1, 2 as t(id);
create temporary view t2 as select * from values 0, 1 as t(id);

-- CTE legacy substitution
SET spark.sql.legacy.ctePrecedence.enabled=true;

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

-- Clean up
DROP VIEW IF EXISTS t;
DROP VIEW IF EXISTS t2;
