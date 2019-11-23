set hive.enforce.bucketing = true;
set hive.exec.reducers.max = 1;

CREATE TABLE bucket3_1(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS;

explain extended
insert overwrite table bucket3_1 partition (ds='1')
select * from src;

insert overwrite table bucket3_1 partition (ds='1')
select * from src;

insert overwrite table bucket3_1 partition (ds='2')
select * from src;

explain
select * from bucket3_1 tablesample (bucket 1 out of 2) s where ds = '1' order by key;

select * from bucket3_1 tablesample (bucket 1 out of 2) s where ds = '1' order by key;
