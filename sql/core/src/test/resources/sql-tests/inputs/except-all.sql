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
SELECT * FROM tab1
EXCEPT ALL
SELECT * FROM tab2;

-- MINUS ALL (synonym for EXCEPT)
SELECT * FROM tab1
MINUS ALL
SELECT * FROM tab2;

-- EXCEPT ALL same table in both branches
SELECT * FROM tab1
EXCEPT ALL
SELECT * FROM tab2 WHERE c1 IS NOT NULL;

-- Empty left relation
SELECT * FROM tab1 WHERE c1 > 5
EXCEPT ALL
SELECT * FROM tab2;

-- Empty right relation
SELECT * FROM tab1
EXCEPT ALL
SELECT * FROM tab2 WHERE c1 > 6;

-- Type Coerced ExceptAll
SELECT * FROM tab1
EXCEPT ALL
SELECT CAST(1 AS BIGINT);

-- Error as types of two side are not compatible
SELECT * FROM tab1
EXCEPT ALL
SELECT array(1);

-- Basic
SELECT * FROM tab3
EXCEPT ALL
SELECT * FROM tab4;

-- Basic
SELECT * FROM tab4
EXCEPT ALL
SELECT * FROM tab3;

-- EXCEPT ALL + INTERSECT
SELECT * FROM tab4
EXCEPT ALL
SELECT * FROM tab3
INTERSECT DISTINCT
SELECT * FROM tab4;

-- EXCEPT ALL + EXCEPT
SELECT * FROM tab4
EXCEPT ALL
SELECT * FROM tab3
EXCEPT DISTINCT
SELECT * FROM tab4;

-- Chain of set operations
SELECT * FROM tab3
EXCEPT ALL
SELECT * FROM tab4
UNION ALL
SELECT * FROM tab3
EXCEPT DISTINCT
SELECT * FROM tab4;

-- Mismatch on number of columns across both branches
SELECT k FROM tab3
EXCEPT ALL
SELECT k, v FROM tab4;

-- Chain of set operations
SELECT * FROM tab3
EXCEPT ALL
SELECT * FROM tab4
UNION
SELECT * FROM tab3
EXCEPT DISTINCT
SELECT * FROM tab4;

-- Using MINUS ALL
SELECT * FROM tab3
MINUS ALL
SELECT * FROM tab4
UNION
SELECT * FROM tab3
MINUS DISTINCT
SELECT * FROM tab4;

-- Chain of set operations
SELECT * FROM tab3
EXCEPT ALL
SELECT * FROM tab4
EXCEPT DISTINCT
SELECT * FROM tab3
EXCEPT DISTINCT
SELECT * FROM tab4;

-- Join under except all. Should produce empty resultset since both left and right sets 
-- are same.
SELECT * 
FROM   (SELECT tab3.k, 
               tab4.v 
        FROM   tab3 
               JOIN tab4 
                 ON tab3.k = tab4.k)
EXCEPT ALL 
SELECT * 
FROM   (SELECT tab3.k, 
               tab4.v 
        FROM   tab3 
               JOIN tab4 
                 ON tab3.k = tab4.k);

-- Join under except all (2)
SELECT * 
FROM   (SELECT tab3.k, 
               tab4.v 
        FROM   tab3 
               JOIN tab4 
                 ON tab3.k = tab4.k) 
EXCEPT ALL 
SELECT * 
FROM   (SELECT tab4.v AS k, 
               tab3.k AS v 
        FROM   tab3 
               JOIN tab4 
                 ON tab3.k = tab4.k);

-- Group by under ExceptAll
SELECT v FROM tab3 GROUP BY v
EXCEPT ALL
SELECT k FROM tab4 GROUP BY k;

-- Clean-up 
DROP VIEW IF EXISTS tab1;
DROP VIEW IF EXISTS tab2;
DROP VIEW IF EXISTS tab3;
DROP VIEW IF EXISTS tab4;
