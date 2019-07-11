-- This test file was converted from intersect-all.sql.
-- Note that currently registered UDF returns a string. So there are some differences, for instance
-- in string cast within UDF in Scala and Python.

CREATE TEMPORARY VIEW tab1 AS SELECT * FROM VALUES
    (1, 2), 
    (1, 2),
    (1, 3),
    (1, 3),
    (2, 3),
    (null, null),
    (null, null)
    AS tab1(k, v);
CREATE TEMPORARY VIEW tab2 AS SELECT * FROM VALUES
    (1, 2), 
    (1, 2), 
    (2, 3),
    (3, 4),
    (null, null),
    (null, null)
    AS tab2(k, v);

-- Basic INTERSECT ALL
SELECT * FROM tab1
INTERSECT ALL
SELECT * FROM tab2;

-- INTERSECT ALL same table in both branches
SELECT * FROM tab1
INTERSECT ALL
SELECT * FROM tab1 WHERE udf(k) = 1;

-- Empty left relation
SELECT * FROM tab1 WHERE k > udf(2)
INTERSECT ALL
SELECT * FROM tab2;

-- Empty right relation
SELECT * FROM tab1
INTERSECT ALL
SELECT * FROM tab2 WHERE CAST(udf(k) AS BIGINT) > CAST(udf(3) AS BIGINT);

-- Type Coerced INTERSECT ALL
SELECT * FROM tab1
INTERSECT ALL
SELECT CAST(udf(1) AS BIGINT), CAST(udf(2) AS BIGINT);

-- Error as types of two side are not compatible
SELECT * FROM tab1
INTERSECT ALL
SELECT array(1), udf(2);

-- Mismatch on number of columns across both branches
SELECT udf(k) FROM tab1
INTERSECT ALL
SELECT udf(k), udf(v) FROM tab2;

-- Basic
SELECT * FROM tab2
INTERSECT ALL
SELECT * FROM tab1
INTERSECT ALL
SELECT * FROM tab2;

-- Chain of different `set operations
SELECT * FROM tab1
EXCEPT
SELECT * FROM tab2
UNION ALL
SELECT * FROM tab1
INTERSECT ALL
SELECT * FROM tab2
;

-- Chain of different `set operations
SELECT * FROM tab1
EXCEPT
SELECT * FROM tab2
EXCEPT
SELECT * FROM tab1
INTERSECT ALL
SELECT * FROM tab2
;

-- test use parenthesis to control order of evaluation
(
  (
    (
      SELECT * FROM tab1
      EXCEPT
      SELECT * FROM tab2
    )
    EXCEPT
    SELECT * FROM tab1
  )
  INTERSECT ALL
  SELECT * FROM tab2
)
;

-- Join under intersect all
SELECT * 
FROM   (SELECT udf(tab1.k),
               udf(tab2.v)
        FROM   tab1 
               JOIN tab2 
                 ON CAST(udf(tab1.k) AS BIGINT) = CAST(udf(tab2.k) AS BIGINT))
INTERSECT ALL 
SELECT * 
FROM   (SELECT udf(tab1.k),
               udf(tab2.v)
        FROM   tab1 
               JOIN tab2 
                 ON CAST(udf(tab1.k) AS BIGINT) = CAST(udf(tab2.k) AS BIGINT));

-- Join under intersect all (2)
SELECT * 
FROM   (SELECT udf(tab1.k),
               udf(tab2.v)
        FROM   tab1 
               JOIN tab2 
                 ON CAST(udf(tab1.k) AS BIGINT) = CAST(udf(tab2.k) AS BIGINT))
INTERSECT ALL 
SELECT * 
FROM   (SELECT udf(tab2.v) AS k,
               udf(tab1.k) AS v
        FROM   tab1 
               JOIN tab2 
                 ON CAST(udf(tab1.k) AS BIGINT) = CAST(udf(tab2.k) AS BIGINT));

-- Group by under intersect all
SELECT CAST(udf(v) AS BIGINT) FROM tab1 GROUP BY v
INTERSECT ALL
SELECT CAST(udf(k) AS BIGINT) FROM tab2 GROUP BY k;

-- Test pre spark2.4 behaviour of set operation precedence
-- All the set operators are given equal precedence and are evaluated
-- from left to right as they appear in the query.

-- Set the property
SET spark.sql.legacy.setopsPrecedence.enabled= true;

SELECT * FROM tab1
EXCEPT
SELECT * FROM tab2
UNION ALL
SELECT * FROM tab1
INTERSECT ALL
SELECT * FROM tab2;

SELECT * FROM tab1
EXCEPT
SELECT * FROM tab2
UNION ALL
SELECT * FROM tab1
INTERSECT
SELECT * FROM tab2;

-- Restore the property
SET spark.sql.legacy.setopsPrecedence.enabled = false;

-- Clean-up 
DROP VIEW IF EXISTS tab1;
DROP VIEW IF EXISTS tab2;
