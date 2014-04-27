set hive.fetch.task.conversion=minimal;

-- backward compatible (minimal)
explain select * from src limit 10;
select * from src limit 10;

explain select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;
select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;

-- negative, select expression
explain select key from src limit 10;
select key from src limit 10;

-- negative, filter on non-partition column
explain select * from srcpart where key > 100 limit 10;
select * from srcpart where key > 100 limit 10;

-- negative, table sampling
explain select * from src TABLESAMPLE (0.25 PERCENT) limit 10;
select * from src TABLESAMPLE (0.25 PERCENT) limit 10;

set hive.fetch.task.conversion=more;

-- backward compatible (more)
explain select * from src limit 10;
select * from src limit 10;

explain select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;
select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;

-- select expression
explain select cast(key as int) * 10, upper(value) from src limit 10;
select cast(key as int) * 10, upper(value) from src limit 10;

-- filter on non-partition column
explain select key from src where key < 100 limit 10;
select key from src where key < 100 limit 10;

-- select expr for partitioned table
explain select key from srcpart where ds='2008-04-08' AND hr='11' limit 10;
select key from srcpart where ds='2008-04-08' AND hr='11' limit 10;

-- virtual columns
explain select *, BLOCK__OFFSET__INSIDE__FILE from src where key < 10 limit 10;
select *, BLOCK__OFFSET__INSIDE__FILE from src where key < 100 limit 10;

-- virtual columns on partitioned table
explain select *, BLOCK__OFFSET__INSIDE__FILE from srcpart where key < 10 limit 30;
select *, BLOCK__OFFSET__INSIDE__FILE from srcpart where key < 10 limit 30;

-- bucket sampling
explain select *, BLOCK__OFFSET__INSIDE__FILE from src TABLESAMPLE (BUCKET 1 OUT OF 40 ON key);
select *, BLOCK__OFFSET__INSIDE__FILE from src TABLESAMPLE (BUCKET 1 OUT OF 40 ON key);
explain select *, BLOCK__OFFSET__INSIDE__FILE from srcpart TABLESAMPLE (BUCKET 1 OUT OF 40 ON key);
select *, BLOCK__OFFSET__INSIDE__FILE from srcpart TABLESAMPLE (BUCKET 1 OUT OF 40 ON key);

-- split sampling
explain select * from src TABLESAMPLE (0.25 PERCENT);
select * from src TABLESAMPLE (0.25 PERCENT);
explain select *, BLOCK__OFFSET__INSIDE__FILE from srcpart TABLESAMPLE (0.25 PERCENT);
select *, BLOCK__OFFSET__INSIDE__FILE from srcpart TABLESAMPLE (0.25 PERCENT);

-- non deterministic func
explain select key, value, BLOCK__OFFSET__INSIDE__FILE from srcpart where ds="2008-04-09" AND rand() > 1;
select key, value, BLOCK__OFFSET__INSIDE__FILE from srcpart where ds="2008-04-09" AND rand() > 1;

-- negative, groupby
explain select key, count(value) from src group by key;

-- negative, distinct
explain select distinct key, value from src;

-- negative, CTAS
explain create table srcx as select distinct key, value from src;

-- negative, analyze
explain analyze table src compute statistics;

-- negative, subq
explain select a.* from (select * from src) a;

-- negative, join
explain select * from src join src src2 on src.key=src2.key;
