set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.txn.testing=true;

create database D1;

use D1;

create table T1(key string, val string) stored as textfile;

alter table T1 compact 'major';

alter table T1 compact 'minor';

drop table T1;
