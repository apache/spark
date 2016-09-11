set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.txn.testing=true;

create table T1(key string, val string) partitioned by (ds string) stored as textfile;

alter table T1 add partition (ds = 'today');
alter table T1 add partition (ds = 'yesterday');

alter table T1 partition (ds = 'today') compact 'major';

alter table T1 partition (ds = 'yesterday') compact 'minor';

drop table T1;
