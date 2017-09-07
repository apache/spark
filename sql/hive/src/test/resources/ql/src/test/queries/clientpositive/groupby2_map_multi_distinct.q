set hive.map.aggr=true;
set hive.groupby.skewindata=false;
set mapreduce.job.reduces=31;

CREATE TABLE dest1(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1);

SELECT dest1.* FROM dest1 ORDER BY key;

-- HIVE-5560 when group by key is used in distinct funtion, invalid result are returned

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr(src.key,1,1), count(DISTINCT substr(src.key,1,1)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr(src.key,1,1), count(DISTINCT substr(src.key,1,1)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1);

SELECT dest1.* FROM dest1 ORDER BY key;
