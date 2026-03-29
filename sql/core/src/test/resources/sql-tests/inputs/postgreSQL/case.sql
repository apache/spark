--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- CASE
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/case.sql
-- Test the CASE statement
--
CREATE TABLE CASE_TBL (
  i integer,
  f double
) USING parquet;

CREATE TABLE CASE2_TBL (
  i integer,
  j integer
) USING parquet;

INSERT INTO CASE_TBL VALUES (1, 10.1);
INSERT INTO CASE_TBL VALUES (2, 20.2);
INSERT INTO CASE_TBL VALUES (3, -30.3);
INSERT INTO CASE_TBL VALUES (4, NULL);

INSERT INTO CASE2_TBL VALUES (1, -1);
INSERT INTO CASE2_TBL VALUES (2, -2);
INSERT INTO CASE2_TBL VALUES (3, -3);
INSERT INTO CASE2_TBL VALUES (2, -4);
INSERT INTO CASE2_TBL VALUES (1, NULL);
INSERT INTO CASE2_TBL VALUES (NULL, -6);

--
-- Simplest examples without tables
--

SELECT '3' AS `One`,
  CASE
    WHEN 1 < 2 THEN 3
  END AS `Simple WHEN`;

SELECT '<NULL>' AS `One`,
  CASE
    WHEN 1 > 2 THEN 3
  END AS `Simple default`;

SELECT '3' AS `One`,
  CASE
    WHEN 1 < 2 THEN 3
    ELSE 4
  END AS `Simple ELSE`;

SELECT '4' AS `One`,
  CASE
    WHEN 1 > 2 THEN 3
    ELSE 4
  END AS `ELSE default`;

SELECT '6' AS `One`,
  CASE
    WHEN 1 > 2 THEN 3
    WHEN 4 < 5 THEN 6
    ELSE 7
  END AS `Two WHEN with default`;

SELECT '7' AS `None`,
  CASE WHEN rand() < 0 THEN 1
  END AS `NULL on no matches`;

-- Constant-expression folding shouldn't evaluate unreachable subexpressions
SELECT CASE WHEN 1=0 THEN 1/0 WHEN 1=1 THEN 1 ELSE 2/0 END;
SELECT CASE 1 WHEN 0 THEN 1/0 WHEN 1 THEN 1 ELSE 2/0 END;

-- However we do not currently suppress folding of potentially
-- reachable subexpressions
SELECT CASE WHEN i > 100 THEN 1/0 ELSE 0 END FROM case_tbl;

-- Test for cases involving untyped literals in test expression
SELECT CASE 'a' WHEN 'a' THEN 1 ELSE 2 END;

--
-- Examples of targets involving tables
--

SELECT '' AS `Five`,
  CASE
    WHEN i >= 3 THEN i
  END AS `>= 3 or Null`
  FROM CASE_TBL;

SELECT '' AS `Five`,
  CASE WHEN i >= 3 THEN (i + i)
       ELSE i
  END AS `Simplest Math`
  FROM CASE_TBL;

SELECT '' AS `Five`, i AS `Value`,
  CASE WHEN (i < 0) THEN 'small'
       WHEN (i = 0) THEN 'zero'
       WHEN (i = 1) THEN 'one'
       WHEN (i = 2) THEN 'two'
       ELSE 'big'
  END AS `Category`
  FROM CASE_TBL;

SELECT '' AS `Five`,
  CASE WHEN ((i < 0) or (i < 0)) THEN 'small'
       WHEN ((i = 0) or (i = 0)) THEN 'zero'
       WHEN ((i = 1) or (i = 1)) THEN 'one'
       WHEN ((i = 2) or (i = 2)) THEN 'two'
       ELSE 'big'
  END AS `Category`
  FROM CASE_TBL;

--
-- Examples of qualifications involving tables
--

--
-- NULLIF() and COALESCE()
-- Shorthand forms for typical CASE constructs
--  defined in the SQL standard.
--

SELECT * FROM CASE_TBL WHERE COALESCE(f,i) = 4;

SELECT * FROM CASE_TBL WHERE NULLIF(f,i) = 2;

SELECT COALESCE(a.f, b.i, b.j)
  FROM CASE_TBL a, CASE2_TBL b;

SELECT *
  FROM CASE_TBL a, CASE2_TBL b
  WHERE COALESCE(a.f, b.i, b.j) = 2;

SELECT '' AS Five, NULLIF(a.i,b.i) AS `NULLIF(a.i,b.i)`,
  NULLIF(b.i, 4) AS `NULLIF(b.i,4)`
  FROM CASE_TBL a, CASE2_TBL b;

SELECT '' AS `Two`, *
  FROM CASE_TBL a, CASE2_TBL b
  WHERE COALESCE(f,b.i) = 2;

-- We don't support update now.
--
-- Examples of updates involving tables
--

-- UPDATE CASE_TBL
--   SET i = CASE WHEN i >= 3 THEN (- i)
--                 ELSE (2 * i) END;

-- SELECT * FROM CASE_TBL;

-- UPDATE CASE_TBL
--   SET i = CASE WHEN i >= 2 THEN (2 * i)
--                 ELSE (3 * i) END;

-- SELECT * FROM CASE_TBL;

-- UPDATE CASE_TBL
--   SET i = CASE WHEN b.i >= 2 THEN (2 * j)
--                 ELSE (3 * j) END
--   FROM CASE2_TBL b
--   WHERE j = -CASE_TBL.i;

-- SELECT * FROM CASE_TBL;

--
-- Nested CASE expressions
--

-- This test exercises a bug caused by aliasing econtext->caseValue_isNull
-- with the isNull argument of the inner CASE's CaseExpr evaluation.  After
-- evaluating the vol(null) expression in the inner CASE's second WHEN-clause,
-- the isNull flag for the case test value incorrectly became true, causing
-- the third WHEN-clause not to match.  The volatile function calls are needed
-- to prevent constant-folding in the planner, which would hide the bug.

-- Wrap this in a single transaction so the transient '=' operator doesn't
-- cause problems in concurrent sessions
-- BEGIN;

-- CREATE FUNCTION vol(text) returns text as
--   'begin return $1; end' language plpgsql volatile;

SELECT CASE
  (CASE vol('bar')
    WHEN 'foo' THEN 'it was foo!'
    WHEN vol(null) THEN 'null input'
    WHEN 'bar' THEN 'it was bar!' END
  )
  WHEN 'it was foo!' THEN 'foo recognized'
  WHEN 'it was bar!' THEN 'bar recognized'
  ELSE 'unrecognized' END;

-- We don't support the features below:
-- 1. CREATE DOMAIN ...
-- 2. CREATE OPERATOR ...
-- 3. CREATE TYPE ...

-- In this case, we can't inline the SQL function without confusing things.
-- CREATE DOMAIN foodomain AS text;

-- CREATE FUNCTION volfoo(text) returns foodomain as
--   'begin return $1::foodomain; end' language plpgsql volatile;

-- CREATE FUNCTION inline_eq(foodomain, foodomain) returns boolean as
--   'SELECT CASE $2::text WHEN $1::text THEN true ELSE false END' language sql;

-- CREATE OPERATOR = (procedure = inline_eq,
--                    leftarg = foodomain, rightarg = foodomain);

-- SELECT CASE volfoo('bar') WHEN 'foo'::foodomain THEN 'is foo' ELSE 'is not foo' END;

-- ROLLBACK;

-- Test multiple evaluation of a CASE arg that is a read/write object (#14472)
-- Wrap this in a single transaction so the transient '=' operator doesn't
-- cause problems in concurrent sessions
-- BEGIN;

-- CREATE DOMAIN arrdomain AS int[];

-- CREATE FUNCTION make_ad(int,int) returns arrdomain as
--   'declare x arrdomain;
--    begin
--      x := array[$1,$2];
--      return x;
--    end' language plpgsql volatile;

-- CREATE FUNCTION ad_eq(arrdomain, arrdomain) returns boolean as
--   'begin return array_eq($1, $2); end' language plpgsql;

-- CREATE OPERATOR = (procedure = ad_eq,
--                    leftarg = arrdomain, rightarg = arrdomain);

-- SELECT CASE make_ad(1,2)
--   WHEN array[2,4]::arrdomain THEN 'wrong'
--   WHEN array[2,5]::arrdomain THEN 'still wrong'
--   WHEN array[1,2]::arrdomain THEN 'right'
--   END;

-- ROLLBACK;

-- Test interaction of CASE with ArrayCoerceExpr (bug #15471)
-- BEGIN;

-- CREATE TYPE casetestenum AS ENUM ('e', 'f', 'g');

-- SELECT
--   CASE 'foo'::text
--     WHEN 'foo' THEN ARRAY['a', 'b', 'c', 'd'] || enum_range(NULL::casetestenum)::text[]
--     ELSE ARRAY['x', 'y']
--     END;

-- ROLLBACK;

--
-- Clean up
--

DROP TABLE CASE_TBL;
DROP TABLE CASE2_TBL;
