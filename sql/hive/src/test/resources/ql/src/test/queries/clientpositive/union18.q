CREATE TABLE DEST1(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

-- union case:map-reduce sub-queries followed by multi-table insert 

explain 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION  ALL  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1 SELECT unionsrc.key, unionsrc.value
INSERT OVERWRITE TABLE DEST2 SELECT unionsrc.key, unionsrc.value, unionsrc.value;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION  ALL  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1 SELECT unionsrc.key, unionsrc.value
INSERT OVERWRITE TABLE DEST2 SELECT unionsrc.key, unionsrc.value, unionsrc.value;

SELECT DEST1.* FROM DEST1 SORT BY DEST1.key, DEST1.value;
SELECT DEST2.* FROM DEST2 SORT BY DEST2.key, DEST2.val1, DEST2.val2;
