set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter,org.apache.hadoop.hive.ql.hooks.PrintCompletedTasksHook;
set hive.auto.convert.join=true;
set hive.auto.convert.join.use.nonstaged=true;

set hive.auto.convert.join.noconditionaltask.size=100;

explain
select a.* from src a join src b on a.key=b.key limit 40;

select a.* from src a join src b on a.key=b.key limit 40;

explain
select a.* from src a join src b on a.key=b.key join src c on a.value=c.value limit 40;

select a.* from src a join src b on a.key=b.key join src c on a.value=c.value limit 40;

set hive.auto.convert.join.noconditionaltask.size=100;

explain
select a.* from src a join src b on a.key=b.key join src c on a.value=c.value where a.key>100 limit 40;

select a.* from src a join src b on a.key=b.key join src c on a.value=c.value where a.key>100 limit 40;

set hive.mapjoin.localtask.max.memory.usage = 0.0001;
set hive.mapjoin.check.memory.rows = 2;

-- fallback to common join
select a.* from src a join src b on a.key=b.key join src c on a.value=c.value where a.key>100 limit 40;

