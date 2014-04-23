set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;

CREATE TABLE tbl1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

insert overwrite table tbl1 select * from src where key < 20;
insert overwrite table tbl2 select * from src where key < 10;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.auto.convert.sortmerge.join.to.mapjoin=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.join=true;

-- Since tbl1 is the bigger table, tbl1 Left Outer Join tbl2 can be performed
explain
select count(*) FROM tbl1 a LEFT OUTER JOIN tbl2 b ON a.key = b.key;
select count(*) FROM tbl1 a LEFT OUTER JOIN tbl2 b ON a.key = b.key;

insert overwrite table tbl2 select * from src where key < 200;

-- Since tbl2 is the bigger table, tbl1 Right Outer Join tbl2 can be performed
explain
select count(*) FROM tbl1 a RIGHT OUTER JOIN tbl2 b ON a.key = b.key;
select count(*) FROM tbl1 a RIGHT OUTER JOIN tbl2 b ON a.key = b.key;
