set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.txn.testing=true;

show locks;

show locks extended;

show locks default;

show transactions;
