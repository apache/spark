set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.map.aggr=false;
set hive.groupby.skewindata=true;

CREATE TABLE dest_g2(key STRING, c1 INT, c2 STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))) GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))) GROUP BY substr(src.key,1,1);

SELECT dest_g2.* FROM dest_g2;
