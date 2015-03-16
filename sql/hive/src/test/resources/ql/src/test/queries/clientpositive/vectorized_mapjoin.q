SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.nonconditionaltask=true;
SET hive.auto.convert.join.nonconditionaltask.size=1000000000;

EXPLAIN SELECT COUNT(t1.cint), MAX(t2.cint), MIN(t1.cint), AVG(t1.cint+t2.cint)
  FROM alltypesorc t1
  JOIN alltypesorc t2 ON t1.cint = t2.cint;

SELECT COUNT(t1.cint), MAX(t2.cint), MIN(t1.cint), AVG(t1.cint+t2.cint)
  FROM alltypesorc t1
  JOIN alltypesorc t2 ON t1.cint = t2.cint;  