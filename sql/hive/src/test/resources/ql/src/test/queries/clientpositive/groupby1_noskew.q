set hive.map.aggr=false;
set hive.groupby.skewindata=false;
set mapreduce.job.reduces=31;

CREATE TABLE dest_g1(key INT, value DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest_g1 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key;

FROM src INSERT OVERWRITE TABLE dest_g1 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key;

SELECT dest_g1.* FROM dest_g1 ORDER BY key;
