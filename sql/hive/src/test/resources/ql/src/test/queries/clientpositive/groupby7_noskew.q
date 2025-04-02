set hive.map.aggr=false;
set hive.multigroupby.singlereducer=false;
set hive.groupby.skewindata=false;
set mapreduce.job.reduces=31;

CREATE TABLE DEST1(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2(key INT, value STRING) STORED AS TEXTFILE;

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true; 

EXPLAIN
FROM SRC
INSERT OVERWRITE TABLE DEST1 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key
INSERT OVERWRITE TABLE DEST2 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key;

FROM SRC
INSERT OVERWRITE TABLE DEST1 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key
INSERT OVERWRITE TABLE DEST2 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key;

SELECT DEST1.* FROM DEST1 ORDER BY key;
SELECT DEST2.* FROM DEST2 ORDER BY key;
