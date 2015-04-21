set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists drop_notableunlock;
create table drop_notableunlock (c int);
unlock table drop_notableunlock;
