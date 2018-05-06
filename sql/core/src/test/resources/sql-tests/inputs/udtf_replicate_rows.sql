CREATE TEMPORARY VIEW tab1 AS SELECT * FROM VALUES
    (1, 'row1', 1.1), 
    (2, 'row2', 2.2),
    (0, 'row3', 3.3),
    (-1,'row4', 4.4),
    (null,'row5', 5.5),
    (3, 'row6', null)
    AS tab1(c1, c2, c3);

-- Requires 2 arguments at minimum.
SELECT replicate_rows() FROM tab1;

-- Requires 2 arguments at minimum.
SELECT replicate_rows(c1) FROM tab1;

-- First argument should be a numeric type.
SELECT replicate_rows("abcd", c2) FROM tab1;

-- untyped null first argument
SELECT replicate_rows(null, c2) FROM tab1;

-- c1, c2 replicated c1 times
SELECT replicate_rows(c1, c2) FROM tab1;

-- c1, c2, c2 repeated replicated c1 times
SELECT replicate_rows(c1, c2, c2) FROM tab1;

-- c1, c2, c2, c3 replicated c1 times
SELECT replicate_rows(c1, c2, c2, c2, c3) FROM tab1;

-- Used as a derived table in FROM clause.
SELECT c2, c1
FROM (
  SELECT replicate_rows(c1, c2) AS (c1, c2) FROM tab1
);

-- column expression.
SELECT replicate_rows(c1, concat(c2, '...'), c2) FROM tab1;

-- Clean-up 
DROP VIEW IF EXISTS tab1;
