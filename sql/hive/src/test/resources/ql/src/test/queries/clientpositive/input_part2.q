CREATE TABLE dest1(key INT, value STRING, hr STRING, ds STRING) STORED AS TEXTFILE;
CREATE TABLE dest2(key INT, value STRING, hr STRING, ds STRING) STORED AS TEXTFILE;

EXPLAIN EXTENDED
FROM srcpart
INSERT OVERWRITE TABLE dest1 SELECT srcpart.key, srcpart.value, srcpart.hr, srcpart.ds WHERE srcpart.key < 100 and srcpart.ds = '2008-04-08' and srcpart.hr = '12'
INSERT OVERWRITE TABLE dest2 SELECT srcpart.key, srcpart.value, srcpart.hr, srcpart.ds WHERE srcpart.key < 100 and srcpart.ds = '2008-04-09' and srcpart.hr = '12';

FROM srcpart
INSERT OVERWRITE TABLE dest1 SELECT srcpart.key, srcpart.value, srcpart.hr, srcpart.ds WHERE srcpart.key < 100 and srcpart.ds = '2008-04-08' and srcpart.hr = '12'
INSERT OVERWRITE TABLE dest2 SELECT srcpart.key, srcpart.value, srcpart.hr, srcpart.ds WHERE srcpart.key < 100 and srcpart.ds = '2008-04-09' and srcpart.hr = '12';

SELECT dest1.* FROM dest1 sort by key,value,ds,hr;
SELECT dest2.* FROM dest2 sort by key,value,ds,hr;


