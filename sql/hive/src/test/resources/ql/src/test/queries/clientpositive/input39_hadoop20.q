-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.20, 0.20S)


create table t1(key string, value string) partitioned by (ds string);
create table t2(key string, value string) partitioned by (ds string);

insert overwrite table t1 partition (ds='1')
select key, value from src;

insert overwrite table t1 partition (ds='2')
select key, value from src;

insert overwrite table t2 partition (ds='1')
select key, value from src;

set hive.test.mode=true;
set hive.mapred.mode=strict;
set mapred.job.tracker=localhost:58;
set hive.exec.mode.local.auto=true;

explain
select count(1) from t1 join t2 on t1.key=t2.key where t1.ds='1' and t2.ds='1';

select count(1) from t1 join t2 on t1.key=t2.key where t1.ds='1' and t2.ds='1';

set hive.test.mode=false;
set mapred.job.tracker;



