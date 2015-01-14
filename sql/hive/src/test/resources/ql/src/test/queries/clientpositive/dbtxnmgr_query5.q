set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create database foo;

use foo;

create table T1(key string, val string) partitioned by (ds string) stored as textfile;

alter table T1 add partition (ds='today');

create view V1 as select key from T1;

show tables;

describe T1;

drop view V1;

drop table T1;

show databases;

drop database foo;
