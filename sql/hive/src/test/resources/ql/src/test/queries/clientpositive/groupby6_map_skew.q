set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set mapreduce.job.reduces=31;

CREATE TABLE dest1(c1 STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT DISTINCT substr(src.value,5,1);

FROM src
INSERT OVERWRITE TABLE dest1 SELECT DISTINCT substr(src.value,5,1);

SELECT dest1.* FROM dest1 ORDER BY c1;


