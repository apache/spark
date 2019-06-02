--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- BOOLEAN
-- https://github.com/postgres/postgres/blob/REL_12_BETA1/src/test/regress/sql/boolean.sql

--
-- sanity check - if this fails go insane!
--
SELECT 1 AS one;


-- ******************testing built-in type bool********************

-- check bool input syntax

SELECT true AS true;

SELECT false AS false;

SELECT cast('t' as boolean) AS true;

SELECT cast('   f           ' as boolean) AS false;

SELECT cast('true' as boolean) AS true;

SELECT cast('test' as boolean) AS error;

SELECT cast('false' as boolean) AS false;

SELECT cast('foo' as boolean) AS error;

SELECT cast('y' as boolean) AS true;

SELECT cast('yes' as boolean) AS true;

SELECT cast('yeah' as boolean) AS error;

SELECT cast('n' as boolean) AS false;

SELECT cast('no' as boolean) AS false;

SELECT cast('nay' as boolean) AS error;

SELECT cast('on' as boolean) AS true;

SELECT cast('off' as boolean) AS false;

SELECT cast('of' as boolean) AS false;

SELECT cast('o' as boolean) AS error;

SELECT cast('on_' as boolean) AS error;

SELECT cast('off_' as boolean) AS error;

SELECT cast('1' as boolean) AS true;

SELECT cast('11' as boolean) AS error;

SELECT cast('0' as boolean) AS false;

SELECT cast('000' as boolean) AS error;

SELECT cast('' as boolean) AS error;

-- and, or, not in qualifications

SELECT cast('t' as boolean) or cast('f' as boolean) AS true;

SELECT cast('t' as boolean) and cast('f' as boolean) AS false;

SELECT not cast('f' as boolean) AS true;

SELECT cast('t' as boolean) = cast('f' as boolean) AS false;

SELECT cast('t' as boolean) <> cast('f' as boolean) AS true;

SELECT cast('t' as boolean) > cast('f' as boolean) AS true;

SELECT cast('t' as boolean) >= cast('f' as boolean) AS true;

SELECT cast('f' as boolean) < cast('t' as boolean) AS true;

SELECT cast('f' as boolean) <= cast('t' as boolean) AS true;

-- explicit casts to/from text
SELECT cast(cast('TrUe' as string) as boolean) AS true, cast(cast('fAlse' as string) as boolean) AS false;
SELECT cast(cast('    true   ' as string) as boolean) AS true,
       cast(cast('     FALSE' as string) as boolean) AS false;
SELECT cast(cast(true as boolean) as string) AS true, cast(cast(false as boolean) as string) AS false;

SELECT cast(cast('  tru e ' as string) as boolean) AS invalid;    -- error
SELECT cast(cast('' as string) as boolean) AS invalid;            -- error

CREATE TABLE BOOLTBL1 (f1 boolean) USING parquet;

INSERT INTO BOOLTBL1 VALUES (cast('t' as boolean));

INSERT INTO BOOLTBL1 VALUES (cast('True' as boolean));

INSERT INTO BOOLTBL1 VALUES (cast('true' as boolean));


-- BOOLTBL1 should be full of true's at this point
SELECT '' AS t_3, BOOLTBL1.* FROM BOOLTBL1;


SELECT '' AS t_3, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = cast('true' as boolean);


SELECT '' AS t_3, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 <> cast('false' as boolean);

-- SELECT '' AS zero, BOOLTBL1.*
--    FROM BOOLTBL1
--    WHERE booleq(cast('false' as boolean), f1);

INSERT INTO BOOLTBL1 VALUES (cast('f' as boolean));

SELECT '' AS f_1, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = cast('false' as boolean);


CREATE TABLE BOOLTBL2 (f1 boolean) USING parquet;

INSERT INTO BOOLTBL2 VALUES (cast('f' as boolean));

INSERT INTO BOOLTBL2 VALUES (cast('false' as boolean));

INSERT INTO BOOLTBL2 VALUES (cast('False' as boolean));

INSERT INTO BOOLTBL2 VALUES (cast('FALSE' as boolean));

-- This is now an invalid expression
-- For pre-v6.3 this evaluated to false - thomas 1997-10-23
INSERT INTO BOOLTBL2
   VALUES (cast('XXX' as boolean));

-- BOOLTBL2 should be full of false's at this point
SELECT '' AS f_4, BOOLTBL2.* FROM BOOLTBL2;


SELECT '' AS tf_12, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 <> BOOLTBL1.f1;


--SELECT '' AS tf_12, BOOLTBL1.*, BOOLTBL2.*
--   FROM BOOLTBL1, BOOLTBL2
--   WHERE boolne(BOOLTBL2.f1,BOOLTBL1.f1);


SELECT '' AS ff_4, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 and BOOLTBL1.f1 = cast('false' as boolean);


SELECT '' AS tf_12_ff_4, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 or BOOLTBL1.f1 = cast('true' as boolean)
   ORDER BY BOOLTBL1.f1, BOOLTBL2.f1;

--
-- SQL syntax
-- Try all combinations to ensure that we get nothing when we expect nothing
-- - thomas 2000-01-04
--

-- SELECT '' AS True, f1
--    FROM BOOLTBL1
--    WHERE f1 IS TRUE;

-- SELECT '' AS "Not False", f1
--    FROM BOOLTBL1
--    WHERE f1 IS NOT FALSE;

-- SELECT '' AS "False", f1
--    FROM BOOLTBL1
--    WHERE f1 IS FALSE;

-- SELECT '' AS "Not True", f1
--    FROM BOOLTBL1
--    WHERE f1 IS NOT TRUE;

-- SELECT '' AS "True", f1
--    FROM BOOLTBL2
--    WHERE f1 IS TRUE;

-- SELECT '' AS "Not False", f1
--    FROM BOOLTBL2
--    WHERE f1 IS NOT FALSE;

-- SELECT '' AS "False", f1
--    FROM BOOLTBL2
--    WHERE f1 IS FALSE;

-- SELECT '' AS "Not True", f1
--    FROM BOOLTBL2
--    WHERE f1 IS NOT TRUE;

--
-- Tests for BooleanTest
--
CREATE TABLE BOOLTBL3 (d string, b boolean, o int) USING parquet;
INSERT INTO BOOLTBL3 VALUES ('true', true, 1);
INSERT INTO BOOLTBL3 VALUES ('false', false, 2);
INSERT INTO BOOLTBL3 VALUES ('null', null, 3);

-- SELECT
--     d,
--     b IS TRUE AS istrue,
--     b IS NOT TRUE AS isnottrue,
--     b IS FALSE AS isfalse,
--     b IS NOT FALSE AS isnotfalse,
--     b IS UNKNOWN AS isunknown,
--     b IS NOT UNKNOWN AS isnotunknown
-- FROM booltbl3 ORDER BY o;


-- Test to make sure short-circuiting and NULL handling is
-- correct. Use a table as source to prevent constant simplification
-- to interfer.
CREATE TABLE booltbl4(isfalse boolean, istrue boolean, isnul boolean) USING parquet;
INSERT INTO booltbl4 VALUES (false, true, null);
-- \pset null '(null)'

-- AND expression need to return null if there's any nulls and not all
-- of the value are true
SELECT istrue AND isnul AND istrue FROM booltbl4;
SELECT istrue AND istrue AND isnul FROM booltbl4;
SELECT isnul AND istrue AND istrue FROM booltbl4;
SELECT isfalse AND isnul AND istrue FROM booltbl4;
SELECT istrue AND isfalse AND isnul FROM booltbl4;
SELECT isnul AND istrue AND isfalse FROM booltbl4;

-- OR expression need to return null if there's any nulls and none
-- of the value is true
SELECT isfalse OR isnul OR isfalse FROM booltbl4;
SELECT isfalse OR isfalse OR isnul FROM booltbl4;
SELECT isnul OR isfalse OR isfalse FROM booltbl4;
SELECT isfalse OR isnul OR istrue FROM booltbl4;
SELECT istrue OR isfalse OR isnul FROM booltbl4;
SELECT isnul OR istrue OR isfalse FROM booltbl4;


--
-- Clean up
-- Many tables are retained by the regression test, but these do not seem
--  particularly useful so just get rid of them for now.
--  - thomas 1997-11-30
--

DROP TABLE  BOOLTBL1;

DROP TABLE  BOOLTBL2;

DROP TABLE  BOOLTBL3;

DROP TABLE  BOOLTBL4;
