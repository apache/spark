-- This test file was converted from except-all.sql.

CREATE TEMPORARY VIEW tab1 AS SELECT * FROM VALUES
    (0), (1), (2), (2), (2), (2), (3), (null), (null) AS tab1(c1);
CREATE TEMPORARY VIEW tab2 AS SELECT * FROM VALUES
    (1), (2), (2), (3), (5), (5), (null) AS tab2(c1);
CREATE TEMPORARY VIEW tab3 AS SELECT * FROM VALUES
    (1, 2), 
    (1, 2),
    (1, 3),
    (2, 3),
    (2, 2)
    AS tab3(k, v);
CREATE TEMPORARY VIEW tab4 AS SELECT * FROM VALUES
    (1, 2), 
    (2, 3),
    (2, 2),
    (2, 2),
    (2, 20)
    AS tab4(k, v);

-- Basic EXCEPT ALL
SELECT udf(c1) FROM tab1
EXCEPT ALL
SELECT udf(c1) FROM tab2;

-- MINUS ALL (synonym for EXCEPT)
SELECT udf(c1) FROM tab1
MINUS ALL
SELECT udf(c1) FROM tab2;

-- EXCEPT ALL same table in both branches
SELECT udf(c1) FROM tab1
EXCEPT ALL
SELECT udf(c1) FROM tab2 WHERE udf(c1) IS NOT NULL;

-- Empty left relation
SELECT udf(c1) FROM tab1 WHERE udf(c1) > 5
EXCEPT ALL
SELECT udf(c1) FROM tab2;

-- Empty right relation
SELECT udf(c1) FROM tab1
EXCEPT ALL
SELECT udf(c1) FROM tab2 WHERE udf(c1 > udf(6));

-- Type Coerced ExceptAll
SELECT udf(c1) FROM tab1
EXCEPT ALL
SELECT CAST(udf(1) AS BIGINT);

-- Error as types of two side are not compatible
SELECT udf(c1) FROM tab1
EXCEPT ALL
SELECT array(1);

-- Basic
SELECT udf(k), v FROM tab3
EXCEPT ALL
SELECT k, udf(v) FROM tab4;

-- Basic
SELECT k, udf(v) FROM tab4
EXCEPT ALL
SELECT udf(k), v FROM tab3;

-- EXCEPT ALL + INTERSECT
SELECT udf(k), udf(v) FROM tab4
EXCEPT ALL
SELECT udf(k), udf(v) FROM tab3
INTERSECT DISTINCT
SELECT udf(k), udf(v) FROM tab4;

-- EXCEPT ALL + EXCEPT
SELECT udf(k), v FROM tab4
EXCEPT ALL
SELECT k, udf(v) FROM tab3
EXCEPT DISTINCT
SELECT udf(k), udf(v) FROM tab4;

-- Chain of set operations
SELECT k, udf(v) FROM tab3
EXCEPT ALL
SELECT udf(k), udf(v) FROM tab4
UNION ALL
SELECT udf(k), v FROM tab3
EXCEPT DISTINCT
SELECT k, udf(v) FROM tab4;

-- Mismatch on number of columns across both branches
SELECT k FROM tab3
EXCEPT ALL
SELECT k, v FROM tab4;

-- Chain of set operations
SELECT udf(k), udf(v) FROM tab3
EXCEPT ALL
SELECT udf(k), udf(v) FROM tab4
UNION
SELECT udf(k), udf(v) FROM tab3
EXCEPT DISTINCT
SELECT udf(k), udf(v) FROM tab4;

-- Using MINUS ALL
SELECT udf(k), udf(v) FROM tab3
MINUS ALL
SELECT k, udf(v) FROM tab4
UNION
SELECT udf(k), udf(v) FROM tab3
MINUS DISTINCT
SELECT k, udf(v) FROM tab4;

-- Chain of set operations
SELECT k, udf(v) FROM tab3
EXCEPT ALL
SELECT udf(k), v FROM tab4
EXCEPT DISTINCT
SELECT k, udf(v) FROM tab3
EXCEPT DISTINCT
SELECT udf(k), v FROM tab4;

-- Join under except all. Should produce empty resultset since both left and right sets 
-- are same.
SELECT * 
FROM   (SELECT tab3.k,
               udf(tab4.v)
        FROM   tab3 
               JOIN tab4 
                 ON udf(tab3.k) = tab4.k)
EXCEPT ALL 
SELECT * 
FROM   (SELECT udf(tab3.k),
               tab4.v
        FROM   tab3 
               JOIN tab4 
                 ON tab3.k = udf(tab4.k));

-- Join under except all (2)
SELECT * 
FROM   (SELECT udf(udf(tab3.k)),
               udf(tab4.v)
        FROM   tab3 
               JOIN tab4 
                 ON udf(udf(tab3.k)) = udf(tab4.k))
EXCEPT ALL 
SELECT * 
FROM   (SELECT udf(tab4.v) AS k,
               udf(udf(tab3.k)) AS v
        FROM   tab3 
               JOIN tab4 
                 ON udf(tab3.k) = udf(tab4.k));

-- Group by under ExceptAll
SELECT udf(v) FROM tab3 GROUP BY v
EXCEPT ALL
SELECT udf(k) FROM tab4 GROUP BY k;

-- Clean-up 
DROP VIEW IF EXISTS tab1;
DROP VIEW IF EXISTS tab2;
DROP VIEW IF EXISTS tab3;
DROP VIEW IF EXISTS tab4;
