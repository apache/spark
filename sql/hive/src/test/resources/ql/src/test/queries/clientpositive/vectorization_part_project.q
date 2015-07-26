SET hive.vectorized.execution.enabled=true;
CREATE TABLE alltypesorc_part(ctinyint tinyint, csmallint smallint, cint int, cbigint bigint, cfloat float, cdouble double, cstring1 string, cstring2 string, ctimestamp1 timestamp, ctimestamp2 timestamp, cboolean1 boolean, cboolean2 boolean) partitioned by (ds string) STORED AS ORC;
insert overwrite table alltypesorc_part partition (ds='2011') select * from alltypesorc limit 100;
insert overwrite table alltypesorc_part partition (ds='2012') select * from alltypesorc limit 100;

explain select (cdouble+2) c1 from alltypesorc_part order by c1 limit 10;
select (cdouble+2) c1 from alltypesorc_part order by c1 limit 10;
