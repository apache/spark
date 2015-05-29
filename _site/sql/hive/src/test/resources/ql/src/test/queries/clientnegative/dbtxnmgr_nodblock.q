set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop database if exists drop_nodblock;
create database drop_nodblock;
lock database drop_nodblock shared;
