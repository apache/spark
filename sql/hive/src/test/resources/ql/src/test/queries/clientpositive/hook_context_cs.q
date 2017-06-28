drop table vcsc;
CREATE TABLE vcsc (c STRING) PARTITIONED BY (ds STRING);
ALTER TABLE vcsc ADD partition (ds='dummy') location '${system:test.tmp.dir}/VerifyContentSummaryCacheHook';

set hive.exec.pre.hooks=org.apache.hadoop.hive.ql.hooks.VerifyContentSummaryCacheHook;
SELECT a.c, b.c FROM vcsc a JOIN vcsc b ON a.ds = 'dummy' AND b.ds = 'dummy' AND a.c = b.c;

set mapreduce.jobtracker.address=local;
set hive.exec.pre.hooks = ;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyContentSummaryCacheHook;
SELECT a.c, b.c FROM vcsc a JOIN vcsc b ON a.ds = 'dummy' AND b.ds = 'dummy' AND a.c = b.c;

set hive.exec.post.hooks=;
drop table vcsc;
