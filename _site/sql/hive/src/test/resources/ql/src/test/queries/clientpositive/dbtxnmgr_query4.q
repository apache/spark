set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

create table T1(key string, val string) stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

select * from T1;

create table T2(key string) partitioned by (val string) stored as textfile;

insert overwrite table T2 partition (val) select key, val from T1;

select * from T2;

drop table T1;
drop table T2;
