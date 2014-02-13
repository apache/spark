set hive.map.aggr=false;
set hive.groupby.skewindata=true;




CREATE TABLE dest1(key STRING, val1 INT, val2 INT) partitioned by (ds string);
CREATE TABLE dest2(key STRING, val1 INT, val2 INT) partitioned by (ds string);

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 partition(ds='111')
  SELECT src.value, count(src.key), count(distinct src.key) GROUP BY src.value
INSERT OVERWRITE TABLE dest2  partition(ds='111')
  SELECT substr(src.value, 5), count(src.key), count(distinct src.key) GROUP BY substr(src.value, 5);

FROM src
INSERT OVERWRITE TABLE dest1 partition(ds='111')
  SELECT src.value, count(src.key), count(distinct src.key) GROUP BY src.value
INSERT OVERWRITE TABLE dest2  partition(ds='111')
  SELECT substr(src.value, 5), count(src.key), count(distinct src.key) GROUP BY substr(src.value, 5);

SELECT * from dest1;
SELECT * from dest2;



