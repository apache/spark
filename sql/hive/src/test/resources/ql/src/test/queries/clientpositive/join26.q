CREATE TABLE dest_j1(key STRING, value STRING, val2 STRING) STORED AS TEXTFILE;

EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.key = z.key and z.ds='2008-04-08' and z.hr=11);

INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.key = z.key and z.ds='2008-04-08' and z.hr=11);

select * from dest_j1 x order by x.key;



