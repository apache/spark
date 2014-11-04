set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.txn.testing=true;

create table T1(key string, val string) stored as textfile;

set hive.txn.testing=true;
alter table T1 compact 'major';

alter table T1 compact 'minor';

drop table T1;
