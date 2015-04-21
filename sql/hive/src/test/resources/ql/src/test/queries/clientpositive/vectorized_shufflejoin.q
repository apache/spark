SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=false;

EXPLAIN SELECT COUNT(t1.cint), MAX(t2.cint), MIN(t1.cint), AVG(t1.cint+t2.cint)
  FROM alltypesorc t1
  JOIN alltypesorc t2 ON t1.cint = t2.cint;

SELECT COUNT(t1.cint), MAX(t2.cint), MIN(t1.cint), AVG(t1.cint+t2.cint)
  FROM alltypesorc t1
  JOIN alltypesorc t2 ON t1.cint = t2.cint;  