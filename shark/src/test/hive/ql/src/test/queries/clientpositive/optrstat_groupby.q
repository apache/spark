SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.OptrStatGroupByHook;
SET hive.exec.mode.local.auto=false;
SET hive.task.progress=true;
-- This test executes the OptrStatGroupBy hook which prints the optr level
-- stats of GROUPBY optr present is the plan of below query
SELECT count(1) FROM src;
