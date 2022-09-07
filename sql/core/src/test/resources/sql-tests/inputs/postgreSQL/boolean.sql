--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- BOOLEAN
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/boolean.sql

--
-- sanity check - if this fails go insane!
--
SELECT 1 AS one;


-- ******************testing built-in type bool********************

-- check bool input syntax

SELECT true AS true;

-- [SPARK-28349] We do not need to follow PostgreSQL to support reserved words in column alias
SELECT false AS `false`;

SELECT boolean('t') AS true;

SELECT boolean('   f           ') AS `false`;

SELECT boolean('true') AS true;

-- [SPARK-27923] PostgreSQL does not accept 'test' but Spark SQL accepts it and sets it to NULL
SELECT boolean('test') AS error;

SELECT boolean('false') AS `false`;

-- [SPARK-27923] PostgreSQL does not accept 'foo' but Spark SQL accepts it and sets it to NULL
SELECT boolean('foo') AS error;

SELECT boolean('y') AS true;

SELECT boolean('yes') AS true;

-- [SPARK-27923] PostgreSQL does not accept 'yeah' but Spark SQL accepts it and sets it to NULL
SELECT boolean('yeah') AS error;

SELECT boolean('n') AS `false`;

SELECT boolean('no') AS `false`;

-- [SPARK-27923] PostgreSQL does not accept 'nay' but Spark SQL accepts it and sets it to NULL
SELECT boolean('nay') AS error;

SELECT boolean('on') AS true;

SELECT boolean('off') AS `false`;

SELECT boolean('of') AS `false`;

-- [SPARK-27923] PostgreSQL does not accept 'o' but Spark SQL accepts it and sets it to NULL
SELECT boolean('o') AS error;

-- [SPARK-27923] PostgreSQL does not accept 'on_' but Spark SQL accepts it and sets it to NULL
SELECT boolean('on_') AS error;

-- [SPARK-27923] PostgreSQL does not accept 'off_' but Spark SQL accepts it and sets it to NULL
SELECT boolean('off_') AS error;

SELECT boolean('1') AS true;

-- [SPARK-27923] PostgreSQL does not accept '11' but Spark SQL accepts it and sets it to NULL
SELECT boolean('11') AS error;

SELECT boolean('0') AS `false`;

-- [SPARK-27923] PostgreSQL does not accept '000' but Spark SQL accepts it and sets it to NULL
SELECT boolean('000') AS error;

-- [SPARK-27923] PostgreSQL does not accept '' but Spark SQL accepts it and sets it to NULL
SELECT boolean('') AS error;

-- and, or, not in qualifications

SELECT boolean('t') or boolean('f') AS true;

SELECT boolean('t') and boolean('f') AS `false`;

SELECT not boolean('f') AS true;

SELECT boolean('t') = boolean('f') AS `false`;

SELECT boolean('t') <> boolean('f') AS true;

SELECT boolean('t') > boolean('f') AS true;

SELECT boolean('t') >= boolean('f') AS true;

SELECT boolean('f') < boolean('t') AS true;

SELECT boolean('f') <= boolean('t') AS true;

-- explicit casts to/from text
SELECT boolean(string('TrUe')) AS true, boolean(string('fAlse')) AS `false`;
SELECT boolean(string('    true   ')) AS true,
       boolean(string('     FALSE')) AS `false`;
SELECT string(boolean(true)) AS true, string(boolean(false)) AS `false`;

-- [SPARK-27923] PostgreSQL does not accept '  tru e ' but Spark SQL accepts it and sets it to NULL
SELECT boolean(string('  tru e ')) AS invalid;    -- error
-- [SPARK-27923] PostgreSQL does not accept '' but Spark SQL accepts it and sets it to NULL
SELECT boolean(string('')) AS invalid;            -- error

CREATE TABLE BOOLTBL1 (f1 boolean) USING parquet;

INSERT INTO BOOLTBL1 VALUES (cast('t' as boolean));

INSERT INTO BOOLTBL1 VALUES (cast('True' as boolean));

INSERT INTO BOOLTBL1 VALUES (cast('true' as boolean));


-- BOOLTBL1 should be full of true's at this point
SELECT '' AS t_3, BOOLTBL1.* FROM BOOLTBL1;


SELECT '' AS t_3, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = boolean('true');


SELECT '' AS t_3, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 <> boolean('false');

SELECT '' AS zero, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE booleq(boolean('false'), f1);

INSERT INTO BOOLTBL1 VALUES (boolean('f'));

SELECT '' AS f_1, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = boolean('false');


CREATE TABLE BOOLTBL2 (f1 boolean) USING parquet;

INSERT INTO BOOLTBL2 VALUES (boolean('f'));

INSERT INTO BOOLTBL2 VALUES (boolean('false'));

INSERT INTO BOOLTBL2 VALUES (boolean('False'));

INSERT INTO BOOLTBL2 VALUES (boolean('FALSE'));

-- [SPARK-27923] PostgreSQL does not accept 'XXX' but Spark SQL accepts it and sets it to NULL
-- This is now an invalid expression
-- For pre-v6.3 this evaluated to false - thomas 1997-10-23
INSERT INTO BOOLTBL2
   VALUES (boolean('XXX'));

-- BOOLTBL2 should be full of false's at this point
SELECT '' AS f_4, BOOLTBL2.* FROM BOOLTBL2;


SELECT '' AS tf_12, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 <> BOOLTBL1.f1;


SELECT '' AS tf_12, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE boolne(BOOLTBL2.f1,BOOLTBL1.f1);


SELECT '' AS ff_4, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 and BOOLTBL1.f1 = boolean('false');


SELECT '' AS tf_12_ff_4, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 or BOOLTBL1.f1 = boolean('true')
   ORDER BY BOOLTBL1.f1, BOOLTBL2.f1;

-- [SPARK-27924] E061-14: Search Conditions
--
-- SQL syntax
-- Try all combinations to ensure that we get nothing when we expect nothing
-- - thomas 2000-01-04
--

SELECT '' AS True, f1
   FROM BOOLTBL1
   WHERE f1 IS TRUE;

SELECT '' AS `Not False`, f1
   FROM BOOLTBL1
   WHERE f1 IS NOT FALSE;

SELECT '' AS `False`, f1
   FROM BOOLTBL1
   WHERE f1 IS FALSE;

SELECT '' AS `Not True`, f1
   FROM BOOLTBL1
   WHERE f1 IS NOT TRUE;

SELECT '' AS `True`, f1
   FROM BOOLTBL2
   WHERE f1 IS TRUE;

SELECT '' AS `Not False`, f1
   FROM BOOLTBL2
   WHERE f1 IS NOT FALSE;

SELECT '' AS `False`, f1
   FROM BOOLTBL2
   WHERE f1 IS FALSE;

SELECT '' AS `Not True`, f1
   FROM BOOLTBL2
   WHERE f1 IS NOT TRUE;

--
-- Tests for BooleanTest
--
CREATE TABLE BOOLTBL3 (d string, b boolean, o int) USING parquet;
INSERT INTO BOOLTBL3 VALUES ('true', true, 1);
INSERT INTO BOOLTBL3 VALUES ('false', false, 2);
INSERT INTO BOOLTBL3 VALUES ('null', null, 3);

-- [SPARK-27924] E061-14: Search Conditions
SELECT
    d,
    b IS TRUE AS istrue,
    b IS NOT TRUE AS isnottrue,
    b IS FALSE AS isfalse,
    b IS NOT FALSE AS isnotfalse,
    b IS UNKNOWN AS isunknown,
    b IS NOT UNKNOWN AS isnotunknown
FROM booltbl3 ORDER BY o;


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
