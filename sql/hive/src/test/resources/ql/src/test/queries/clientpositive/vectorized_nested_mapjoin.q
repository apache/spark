SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.nonconditionaltask=true;
SET hive.auto.convert.join.nonconditionaltask.size=1000000000;

explain select sum(t1.td) from (select  v1.csmallint as tsi, v1.cdouble as td from alltypesorc v1, alltypesorc v2 where v1.ctinyint=v2.ctinyint) t1 join alltypesorc v3 on t1.tsi=v3.csmallint;

select sum(t1.td) from (select  v1.csmallint as tsi, v1.cdouble as td from alltypesorc v1, alltypesorc v2 where v1.ctinyint=v2.ctinyint) t1 join alltypesorc v3 on t1.tsi=v3.csmallint;
