set datanucleus.cache.collections=false;
set hive.stats.autogather=true;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.map.aggr=true;

create table tmptable(key string, value string);

EXPLAIN
INSERT OVERWRITE TABLE tmptable
SELECT unionsrc.key, unionsrc.value 
FROM (SELECT 'tst1' AS key, cast(count(1) AS string) AS value FROM src s1
      UNION  ALL  
      SELECT s2.key AS key, s2.value AS value FROM src1 s2) unionsrc;

INSERT OVERWRITE TABLE tmptable
SELECT unionsrc.key, unionsrc.value 
FROM (SELECT 'tst1' AS key, cast(count(1) AS string) AS value FROM src s1
      UNION  ALL  
      SELECT s2.key AS key, s2.value AS value FROM src1 s2) unionsrc;

SELECT * FROM tmptable x SORT BY x.key, x.value;

DESCRIBE FORMATTED tmptable;

-- Load a file into a existing table
-- Some stats (numFiles, totalSize) should be updated correctly
-- Some other stats (numRows, rawDataSize) should be cleared
load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE tmptable;
DESCRIBE FORMATTED tmptable;