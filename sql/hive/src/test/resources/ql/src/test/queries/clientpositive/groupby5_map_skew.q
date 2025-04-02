set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set mapreduce.job.reduces=31;

CREATE TABLE dest1(key INT) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1 SELECT sum(src.key);

FROM src INSERT OVERWRITE TABLE dest1 SELECT sum(src.key);

SELECT dest1.* FROM dest1;
