-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.20, 0.20S)

CREATE TABLE dest1(c1 INT, c2 STRING) STORED AS TEXTFILE;

set mapreduce.framework.name=yarn;
set mapreduce.jobtracker.address=localhost:58;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.input.files.max=6;

EXPLAIN
FROM src JOIN srcpart ON src.key = srcpart.key AND srcpart.ds = '2008-04-08' and src.key > 100
INSERT OVERWRITE TABLE dest1 SELECT src.key, srcpart.value;

FROM src JOIN srcpart ON src.key = srcpart.key AND srcpart.ds = '2008-04-08' and src.key > 100
INSERT OVERWRITE TABLE dest1 SELECT src.key, srcpart.value;

select dest1.* from dest1;
