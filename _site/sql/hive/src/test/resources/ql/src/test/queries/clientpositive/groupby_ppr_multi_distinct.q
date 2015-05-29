set hive.map.aggr=false;
set hive.groupby.skewindata=false;

CREATE TABLE dest1(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;

EXPLAIN EXTENDED
FROM srcpart src
INSERT OVERWRITE TABLE dest1 
SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(DISTINCT src.value)
WHERE src.ds = '2008-04-08'
GROUP BY substr(src.key,1,1);

FROM srcpart src
INSERT OVERWRITE TABLE dest1 
SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(DISTINCT src.value)
WHERE src.ds = '2008-04-08'
GROUP BY substr(src.key,1,1);

SELECT dest1.* FROM dest1;
