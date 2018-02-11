set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=300;
set mapreduce.input.fileinputformat.split.minsize=300;
set mapreduce.input.fileinputformat.split.minsize.per.node=300;
set mapreduce.input.fileinputformat.split.minsize.per.rack=300;
set hive.exec.mode.local.auto=true;
set hive.merge.smallfiles.avgsize=1;

-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.20, 0.20S)

-- create file inputs
create table sih_i_part (key int, value string) partitioned by (p string);
insert overwrite table sih_i_part partition (p='1') select key, value from src;
insert overwrite table sih_i_part partition (p='2') select key+10000, value from src;
insert overwrite table sih_i_part partition (p='3') select key+20000, value from src;
create table sih_src as select key, value from sih_i_part order by key, value;
create table sih_src2 as select key, value from sih_src order by key, value;

set hive.exec.post.hooks = org.apache.hadoop.hive.ql.hooks.VerifyIsLocalModeHook;
set mapreduce.framework.name=yarn;
set mapreduce.jobtracker.address=localhost:58;
set hive.sample.seednumber=7;

-- Relaxing hive.exec.mode.local.auto.input.files.max=1.
-- Hadoop20 will not generate more splits than there are files (one).
-- Hadoop23 generate splits correctly (four), hence the max needs to be adjusted to ensure running in local mode.
-- Default value is hive.exec.mode.local.auto.input.files.max=4 which produces expected behavior on Hadoop23.
-- hive.sample.seednumber is required because Hadoop23 generates multiple splits and tablesample is non-repeatable without it.

-- sample split, running locally limited by num tasks
select count(1) from sih_src tablesample(1 percent);

-- sample two tables
select count(1) from sih_src tablesample(1 percent) a join sih_src2 tablesample(1 percent) b on a.key = b.key;

set hive.exec.mode.local.auto.inputbytes.max=1000;

-- sample split, running locally limited by max bytes
select count(1) from sih_src tablesample(1 percent);
