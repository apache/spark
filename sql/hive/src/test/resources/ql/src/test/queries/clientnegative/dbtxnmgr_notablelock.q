set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists drop_notablelock;
create table drop_notablelock (c int);
lock table drop_notablelock shared;
