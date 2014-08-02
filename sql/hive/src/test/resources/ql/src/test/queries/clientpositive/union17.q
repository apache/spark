CREATE TABLE DEST1(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

-- union case:map-reduce sub-queries followed by multi-table insert

explain 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION  ALL  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key, unionsrc.value;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION  ALL  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key, unionsrc.value;

SELECT DEST1.* FROM DEST1;
SELECT DEST2.* FROM DEST2;
