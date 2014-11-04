set hive.lock.numretries=1;
set hive.lock.sleep.between.retries=1;
set hive.support.concurrency=true;
set hive.lock.manager=org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager;

drop table if exists drop_with_concurrency_1;
create table drop_with_concurrency_1 (c1 int);
drop table drop_with_concurrency_1;
