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


-- Except operation that will be replaced by Filter: SPARK-22181 - case 1
SELECT * FROM t1 EXCEPT SELECT * FROM t1 where v <> 1 and v <> 2;


-- Except operation that will be replaced by Filter: SPARK-22181 - case 2
SELECT * FROM t1 where v <> 1 and v <> 22 EXCEPT SELECT * FROM t1 where v <> 2 and v >= 3;


-- Except operation that will be replaced by left anti join
SELECT * FROM t2 where v >= 1 and v <> 22 EXCEPT SELECT * FROM t1;
