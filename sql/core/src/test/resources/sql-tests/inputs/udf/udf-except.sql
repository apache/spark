-- This test file was converted from except.sql.
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
SELECT udf(k), udf(v) FROM t1 EXCEPT SELECT udf(k), udf(v) FROM t2;


-- Except operation that will be replaced by Filter: SPARK-22181
SELECT * FROM t1 EXCEPT SELECT * FROM t1 where udf(v) <> 1 and v <> udf(2);


-- Except operation that will be replaced by Filter: SPARK-22181
SELECT * FROM t1 where udf(v) <> 1 and v <> udf(22) EXCEPT SELECT * FROM t1 where udf(v) <> 2 and v >= udf(3);


-- Except operation that will be replaced by Filter: SPARK-22181
SELECT t1.* FROM t1, t2 where t1.k = t2.k
EXCEPT
SELECT t1.* FROM t1, t2 where t1.k = t2.k and t1.k != udf('one');


-- Except operation that will be replaced by left anti join
SELECT * FROM t2 where v >= udf(1) and udf(v) <> 22 EXCEPT SELECT * FROM t1;


-- Except operation that will be replaced by left anti join
SELECT (SELECT min(udf(k)) FROM t2 WHERE t2.k = t1.k) min_t2 FROM t1
MINUS
SELECT (SELECT udf(min(k)) FROM t2) abs_min_t2 FROM t1 WHERE  t1.k = udf('one');


-- Except operation that will be replaced by left anti join
SELECT t1.k
FROM   t1
WHERE  t1.v <= (SELECT   udf(max(udf(t2.v)))
                FROM     t2
                WHERE    udf(t2.k) = udf(t1.k))
MINUS
SELECT t1.k
FROM   t1
WHERE  udf(t1.v) >= (SELECT   min(udf(t2.v))
                FROM     t2
                WHERE    t2.k = t1.k);
