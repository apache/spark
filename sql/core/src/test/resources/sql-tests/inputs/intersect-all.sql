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
SELECT * FROM tab1 WHERE k = 1;

-- Empty left relation
SELECT * FROM tab1 WHERE k > 2
INTERSECT ALL
SELECT * FROM tab2;

-- Empty right relation
SELECT * FROM tab1
INTERSECT ALL
SELECT * FROM tab2 WHERE k > 3;

-- Type Coerced INTERSECT ALL
SELECT * FROM tab1
INTERSECT ALL
SELECT CAST(1 AS BIGINT), CAST(2 AS BIGINT);

-- Error as types of two side are not compatible
SELECT * FROM tab1
INTERSECT ALL
SELECT array(1), 2;

-- Mismatch on number of columns across both branches
SELECT k FROM tab1
INTERSECT ALL
SELECT k, v FROM tab2;

-- Basic
SELECT * FROM tab2
INTERSECT ALL
SELECT * FROM tab1
INTERSECT ALL
SELECT * FROM tab2;

-- Chain of different `set operations
-- We need to parenthesize the following two queries to enforce
-- certain order of evaluation of operators. After fix to
-- SPARK-24966 this can be removed.
SELECT * FROM tab1
EXCEPT
SELECT * FROM tab2
UNION ALL
(
SELECT * FROM tab1
INTERSECT ALL
SELECT * FROM tab2
);

-- Chain of different `set operations
SELECT * FROM tab1
EXCEPT
SELECT * FROM tab2
EXCEPT
(
SELECT * FROM tab1
INTERSECT ALL
SELECT * FROM tab2
);

-- Join under intersect all
SELECT * 
FROM   (SELECT tab1.k, 
               tab2.v 
        FROM   tab1 
               JOIN tab2 
                 ON tab1.k = tab2.k)
INTERSECT ALL 
SELECT * 
FROM   (SELECT tab1.k, 
               tab2.v 
        FROM   tab1 
               JOIN tab2 
                 ON tab1.k = tab2.k);

-- Join under intersect all (2)
SELECT * 
FROM   (SELECT tab1.k, 
               tab2.v 
        FROM   tab1 
               JOIN tab2 
                 ON tab1.k = tab2.k) 
INTERSECT ALL 
SELECT * 
FROM   (SELECT tab2.v AS k, 
               tab1.k AS v 
        FROM   tab1 
               JOIN tab2 
                 ON tab1.k = tab2.k);

-- Group by under intersect all
SELECT v FROM tab1 GROUP BY v
INTERSECT ALL
SELECT k FROM tab2 GROUP BY k;

-- Clean-up 
DROP VIEW IF EXISTS tab1;
DROP VIEW IF EXISTS tab2;
