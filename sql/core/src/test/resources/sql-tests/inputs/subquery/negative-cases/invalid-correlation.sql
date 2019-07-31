-- The test file contains negative test cases
-- of invalid queries where error messages are expected.

CREATE TEMPORARY VIEW t1 AS SELECT * FROM VALUES
  (1, 2, 3)
AS t1(t1a, t1b, t1c);

CREATE TEMPORARY VIEW t2 AS SELECT * FROM VALUES
  (1, 0, 1)
AS t2(t2a, t2b, t2c);

CREATE TEMPORARY VIEW t3 AS SELECT * FROM VALUES
  (3, 1, 2)
AS t3(t3a, t3b, t3c);

-- TC 01.01
-- The column t2b in the SELECT of the subquery is invalid
-- because it is neither an aggregate function nor a GROUP BY column.
SELECT t1a, t2b
FROM   t1, t2
WHERE  t1b = t2c
AND    t2b = (SELECT max(avg)
              FROM   (SELECT   t2b, avg(t2b) avg
                      FROM     t2
                      WHERE    t2a = t1.t1b
                     )
             )
;

-- TC 01.02
-- Invalid due to the column t2b not part of the output from table t2.
SELECT *
FROM   t1
WHERE  t1a IN (SELECT   min(t2a)
               FROM     t2
               GROUP BY t2c
               HAVING   t2c IN (SELECT   max(t3c)
                                FROM     t3
                                GROUP BY t3b
                                HAVING   t3b > t2b ))
;

-- TC 01.03
-- Invalid due to mixure of outer and local references under an AggegatedExpression 
-- in a correlated predicate
SELECT t1a 
FROM   t1
GROUP  BY 1
HAVING EXISTS (SELECT t2a
               FROM  t2
               GROUP BY 1
               HAVING t2a < min(t1a + t2a));

-- TC 01.04
-- Invalid due to mixure of outer and local references under an AggegatedExpression 
SELECT t1a 
FROM   t1
WHERE  t1a IN (SELECT t2a 
               FROM   t2
               WHERE  EXISTS (SELECT 1 
                              FROM   t3
                              GROUP BY 1
                              HAVING min(t2a + t3a) > 1));

-- TC 01.05
-- Invalid due to outer reference appearing in projection list
SELECT t1a 
FROM   t1
WHERE  t1a IN (SELECT t2a 
               FROM   t2
               WHERE  EXISTS (SELECT min(t2a) 
                              FROM   t3));

