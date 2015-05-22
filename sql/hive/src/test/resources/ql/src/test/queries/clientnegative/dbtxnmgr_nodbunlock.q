set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop database if exists drop_nodbunlock;
create database drop_nodbunlock;
unlock database drop_nodbunlock;
