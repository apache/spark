set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter,org.apache.hadoop.hive.ql.hooks.PrintCompletedTasksHook;

set hive.auto.convert.join = true;
set hive.mapjoin.localtask.max.memory.usage = 0.0001;
set hive.mapjoin.check.memory.rows = 2;
set hive.auto.convert.join.noconditionaltask = false;

-- This test tests the scenario when the mapper dies. So, create a conditional task for the mapjoin
CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src2.value 
where (src1.ds = '2008-04-08' or src1.ds = '2008-04-09' )and (src1.hr = '12' or src1.hr = '11');

SELECT sum(hash(dest1.key,dest1.value)) FROM dest1;



CREATE TABLE dest_j2(key INT, value STRING) STORED AS TEXTFILE;

FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest_j2 SELECT src1.key, src3.value;

SELECT sum(hash(dest_j2.key,dest_j2.value)) FROM dest_j2;

CREATE TABLE dest_j1(key INT, value STRING) STORED AS TEXTFILE;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1 SELECT src1.key, src2.value;

SELECT sum(hash(dest_j1.key,dest_j1.value)) FROM dest_j1;

