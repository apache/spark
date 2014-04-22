set hive.enforce.bucketing = true;
set hive.exec.mode.local.auto=false;
set mapred.reduce.tasks = 10;

-- This test sets number of mapred tasks to 10 for a database with 50 buckets, 
-- and uses a post-hook to confirm that 10 tasks were created

CREATE TABLE bucket_nr(key int, value string) CLUSTERED BY (key) INTO 50 BUCKETS;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyNumReducersHook;
set VerifyNumReducersHook.num.reducers=10;

insert overwrite table bucket_nr
select * from src;

set hive.exec.post.hooks=;
drop table bucket_nr;
