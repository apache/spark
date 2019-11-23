CREATE TABLE dest_j1(key STRING, value STRING, val2 STRING) STORED AS TEXTFILE;

EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.key = z.key)
WHERE z.ds='2008-04-08' and z.hr=11;

INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.key = z.key)
WHERE z.ds='2008-04-08' and z.hr=11;

select * from dest_j1 x order by x.key;

CREATE TABLE src_copy(key int, value string);
CREATE TABLE src1_copy(key string, value string);
INSERT OVERWRITE TABLE src_copy select key, value from src;
INSERT OVERWRITE TABLE src1_copy select key, value from src1;

EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src1_copy x JOIN src_copy y ON (x.key = y.key) 
JOIN srcpart z ON (x.key = z.key)
WHERE z.ds='2008-04-08' and z.hr=11;

INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src1_copy x JOIN src_copy y ON (x.key = y.key) 
JOIN srcpart z ON (x.key = z.key)
WHERE z.ds='2008-04-08' and z.hr=11;

select * from dest_j1 x order by x.key;





