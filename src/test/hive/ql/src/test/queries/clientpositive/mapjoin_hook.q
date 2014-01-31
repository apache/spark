set hive.exec.post.hooks = org.apache.hadoop.hive.ql.hooks.MapJoinCounterHook ;
drop table dest1;
CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

set hive.auto.convert.join = true;

INSERT OVERWRITE TABLE dest1 
SELECT /*+ MAPJOIN(x) */ x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key;


FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key = src3.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src3.value;



set hive.mapjoin.localtask.max.memory.usage = 0.0001;
set hive.mapjoin.check.memory.rows = 2;
set hive.auto.convert.join.noconditionaltask = false;


FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src2.value 
where (src1.ds = '2008-04-08' or src1.ds = '2008-04-09' )and (src1.hr = '12' or src1.hr = '11');


FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src3.value;




