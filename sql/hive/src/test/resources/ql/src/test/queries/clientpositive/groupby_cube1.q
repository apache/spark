set hive.map.aggr=true;
set hive.groupby.skewindata=false;

CREATE TABLE T1(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

EXPLAIN
SELECT key, val, count(1) FROM T1 GROUP BY key, val with cube;

SELECT key, val, count(1) FROM T1 GROUP BY key, val with cube
ORDER BY key, val;

EXPLAIN
SELECT key, count(distinct val) FROM T1 GROUP BY key with cube;

SELECT key, count(distinct val) FROM T1 GROUP BY key with cube
ORDER BY key;

set hive.groupby.skewindata=true;

EXPLAIN
SELECT key, val, count(1) FROM T1 GROUP BY key, val with cube;

SELECT key, val, count(1) FROM T1 GROUP BY key, val with cube
ORDER BY key, val;

EXPLAIN
SELECT key, count(distinct val) FROM T1 GROUP BY key with cube;

SELECT key, count(distinct val) FROM T1 GROUP BY key with cube
ORDER BY key;


set hive.multigroupby.singlereducer=true;

CREATE TABLE T2(key1 STRING, key2 STRING, val INT) STORED AS TEXTFILE;
CREATE TABLE T3(key1 STRING, key2 STRING, val INT) STORED AS TEXTFILE;

EXPLAIN
FROM T1
INSERT OVERWRITE TABLE T2 SELECT key, val, count(1) group by key, val with cube
INSERT OVERWRITE TABLE T3 SELECT key, val, sum(1) group by key, val with cube;


FROM T1
INSERT OVERWRITE TABLE T2 SELECT key, val, count(1) group by key, val with cube
INSERT OVERWRITE TABLE T3 SELECT key, val, sum(1) group by key, val with cube;

