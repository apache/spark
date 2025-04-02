set hive.map.aggr=false;

set hive.groupby.skewindata=false;
set mapreduce.job.reduces=31;

CREATE TABLE dest1(c1 STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr(src.key,1,1) GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr(src.key,1,1) GROUP BY substr(src.key,1,1);

SELECT dest1.* FROM dest1 ORDER BY c1;

