-- Tests different scenarios of except operation
create temporary view t1 as select * from values
  ("one", 1),
  ("two", 2),
  ("three", 3),
  ("one", NULL)
  as t1(k, v);

create temporary view t2 as select * from values
  ("one", 1),
  ("two", 22),
  ("one", 5),
  ("one", NULL),
  (NULL, 5)
  as t2(k, v);


-- Except operation that will be replaced by left anti join
SELECT * FROM t1 EXCEPT SELECT * FROM t2;


-- Except operation that will be replaced by Filter: SPARK-22181
SELECT * FROM t1 EXCEPT SELECT * FROM t1 where v <> 1 and v <> 2;


-- Except operation that will be replaced by Filter: SPARK-22181
SELECT * FROM t1 where v <> 1 and v <> 22 EXCEPT SELECT * FROM t1 where v <> 2 and v >= 3;


-- Except operation that will be replaced by Filter: SPARK-22181
SELECT t1.* FROM t1, t2 where t1.k = t2.k
EXCEPT
SELECT t1.* FROM t1, t2 where t1.k = t2.k and t1.k != 'one';


-- Except operation that will be replaced by left anti join
SELECT * FROM t2 where v >= 1 and v <> 22 EXCEPT SELECT * FROM t1;


-- Except operation that will be replaced by left anti join
SELECT (SELECT min(k) FROM t2 WHERE t2.k = t1.k) min_t2 FROM t1
MINUS
SELECT (SELECT min(k) FROM t2) abs_min_t2 FROM t1 WHERE  t1.k = 'one';


-- Except operation that will be replaced by left anti join
SELECT t1.k
FROM   t1
WHERE  t1.v <= (SELECT   max(t2.v)
                FROM     t2
                WHERE    t2.k = t1.k)
MINUS
SELECT t1.k
FROM   t1
WHERE  t1.v >= (SELECT   min(t2.v)
                FROM     t2
                WHERE    t2.k = t1.k);

-- SPARK-32638: corrects references when adding aliases in WidenSetOperationTypes
CREATE OR REPLACE TEMPORARY VIEW t3 AS VALUES (decimal(1)) tbl(v);
SELECT t.v FROM (
  SELECT v FROM t3
  EXCEPT
  SELECT v + v AS v FROM t3
) t;

SELECT SUM(t.v) FROM (
  SELECT v FROM t3
  EXCEPT
  SELECT v + v AS v FROM t3
) t;

-- Clean-up
DROP VIEW IF EXISTS t1;
DROP VIEW IF EXISTS t2;
DROP VIEW IF EXISTS t3;
