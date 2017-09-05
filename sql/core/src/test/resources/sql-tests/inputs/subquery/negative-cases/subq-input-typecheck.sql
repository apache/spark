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
SELECT 
  ( SELECT max(t2b), min(t2b) 
    FROM t2 
    WHERE t2.t2b = t1.t1b
    GROUP BY t2.t2b
  )
FROM t1;

-- TC 01.01
SELECT 
  ( SELECT max(t2b), min(t2b) 
    FROM t2 
    WHERE t2.t2b > 0
    GROUP BY t2.t2b
  )
FROM t1;

-- TC 01.03
SELECT * FROM t1
WHERE
t1a IN (SELECT t2a, t2b 
        FROM t2
        WHERE t1a = t2a);

-- TC 01.04
SELECT * FROM T1 
WHERE
(t1a, t1b) IN (SELECT t2a
               FROM t2
               WHERE t1a = t2a);

