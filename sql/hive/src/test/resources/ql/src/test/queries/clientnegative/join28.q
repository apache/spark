CREATE TABLE dest_j1(key STRING, value STRING) STORED AS TEXTFILE;

-- Mapjoin followed by mapjoin is not supported.
-- The same query would work fine without the hint.
-- Note that there is a positive test with the same name in clientpositive
EXPLAIN
INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(z) */ subq.key1, z.value
FROM
(SELECT /*+ MAPJOIN(x) */ x.key as key1, x.value as value1, y.key as key2, y.value as value2 
 FROM src1 x JOIN src y ON (x.key = y.key)) subq
 JOIN srcpart z ON (subq.key1 = z.key and z.ds='2008-04-08' and z.hr=11);



