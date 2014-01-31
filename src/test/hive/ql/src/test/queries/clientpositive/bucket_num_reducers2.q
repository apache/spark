set hive.enforce.bucketing = true;
set hive.exec.mode.local.auto=false;
set hive.exec.reducers.max = 2;

-- This test sets the maximum number of reduce tasks to 2 for overwriting a
-- table with 3 buckets, and uses a post-hook to confirm that 1 reducer was used

CREATE TABLE test_table(key int, value string) CLUSTERED BY (key) INTO 3 BUCKETS;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyNumReducersHook;
set VerifyNumReducersHook.num.reducers=1;

insert overwrite table test_table
select * from src;
