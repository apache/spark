CREATE TABLE dest_j1(key STRING, value STRING, val2 STRING) STORED AS TEXTFILE;
CREATE TABLE dest_j2(key STRING, value STRING, val2 STRING) STORED AS TEXTFILE;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=6000;

-- Since the inputs are small, it should be automatically converted to mapjoin

EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j1
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.value = z.value and z.ds='2008-04-08' and z.hr=11);

INSERT OVERWRITE TABLE dest_j1
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.value = z.value and z.ds='2008-04-08' and z.hr=11);

select * from dest_j1 x order by x.value;

EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j1
SELECT x.key, z.value, y.value
FROM src w JOIN src1 x ON (x.value = w.value) 
JOIN src y ON (x.key = y.key) 
JOIN src1 z ON (x.key = z.key);

INSERT OVERWRITE TABLE dest_j1
SELECT x.key, z.value, y.value
FROM src w JOIN src1 x ON (x.value = w.value) 
JOIN src y ON (x.key = y.key) 
JOIN src1 z ON (x.key = z.key);

select * from dest_j1 x order by x.value;

EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j2
SELECT res.key, z.value, res.value
FROM (select x.key, x.value from src1 x JOIN src y ON (x.key = y.key)) res 
JOIN srcpart z ON (res.value = z.value and z.ds='2008-04-08' and z.hr=11);

INSERT OVERWRITE TABLE dest_j2
SELECT res.key, z.value, res.value
FROM (select x.key, x.value from src1 x JOIN src y ON (x.key = y.key)) res 
JOIN srcpart z ON (res.value = z.value and z.ds='2008-04-08' and z.hr=11);

select * from dest_j2 x order by x.value;

EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j2
SELECT res.key, z.value, res.value
FROM (select x.key, x.value from src1 x LEFT OUTER JOIN src y ON (x.key = y.key)) res 
JOIN srcpart z ON (res.value = z.value and z.ds='2008-04-08' and z.hr=11);

INSERT OVERWRITE TABLE dest_j2
SELECT res.key, z.value, res.value
FROM (select x.key, x.value from src1 x LEFT OUTER JOIN src y ON (x.key = y.key)) res 
JOIN srcpart z ON (res.value = z.value and z.ds='2008-04-08' and z.hr=11);

select * from dest_j2 x order by x.value;

EXPLAIN
INSERT OVERWRITE TABLE dest_j2
SELECT res.key, x.value, res.value  
FROM (select x.key, x.value from src1 x JOIN src y ON (x.key = y.key)) res 
JOIN srcpart x ON (res.value = x.value and x.ds='2008-04-08' and x.hr=11);

INSERT OVERWRITE TABLE dest_j2
SELECT res.key, x.value, res.value  
FROM (select x.key, x.value from src1 x JOIN src y ON (x.key = y.key)) res 
JOIN srcpart x ON (res.value = x.value and x.ds='2008-04-08' and x.hr=11);

select * from dest_j2 x order by x.value;

EXPLAIN
INSERT OVERWRITE TABLE dest_j2
SELECT res.key, y.value, res.value
FROM (select x.key, x.value from src1 x JOIN src y ON (x.key = y.key)) res 
JOIN srcpart y ON (res.value = y.value and y.ds='2008-04-08' and y.hr=11);

INSERT OVERWRITE TABLE dest_j2
SELECT res.key, y.value, res.value
FROM (select x.key, x.value from src1 x JOIN src y ON (x.key = y.key)) res 
JOIN srcpart y ON (res.value = y.value and y.ds='2008-04-08' and y.hr=11);

select * from dest_j2 x order by x.value;
