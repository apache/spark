CREATE TABLE decimal_mapjoin STORED AS ORC AS 
  SELECT cdouble, CAST (((cdouble*22.1)/37) AS DECIMAL(20,10)) AS cdecimal1, 
  CAST (((cdouble*9.3)/13) AS DECIMAL(23,14)) AS cdecimal2,
  cint
  FROM alltypesorc;
 
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.nonconditionaltask=true;
SET hive.auto.convert.join.nonconditionaltask.size=1000000000;
SET hive.vectorized.execution.enabled=true;

EXPLAIN SELECT l.cint, r.cint, l.cdecimal1, r.cdecimal2
  FROM decimal_mapjoin l
  JOIN decimal_mapjoin r ON l.cint = r.cint
  WHERE l.cint = 6981;
SELECT l.cint, r.cint, l.cdecimal1, r.cdecimal2
  FROM decimal_mapjoin l
  JOIN decimal_mapjoin r ON l.cint = r.cint
  WHERE l.cint = 6981;