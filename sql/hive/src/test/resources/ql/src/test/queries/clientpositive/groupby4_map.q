set hive.map.aggr=true;
set hive.groupby.skewindata=false;
set mapreduce.job.reduces=31;

CREATE TABLE dest1(key INT) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1 SELECT count(1);

FROM src INSERT OVERWRITE TABLE dest1 SELECT count(1);

SELECT dest1.* FROM dest1;
