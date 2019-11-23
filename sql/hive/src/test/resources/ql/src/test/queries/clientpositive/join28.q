CREATE TABLE dest_j1(key STRING, value STRING) STORED AS TEXTFILE;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

-- Since the inputs are small, it should be automatically converted to mapjoin

EXPLAIN
INSERT OVERWRITE TABLE dest_j1 
SELECT subq.key1, z.value
FROM
(SELECT x.key as key1, x.value as value1, y.key as key2, y.value as value2 
 FROM src1 x JOIN src y ON (x.key = y.key)) subq
 JOIN srcpart z ON (subq.key1 = z.key and z.ds='2008-04-08' and z.hr=11);

INSERT OVERWRITE TABLE dest_j1 
SELECT subq.key1, z.value
FROM
(SELECT x.key as key1, x.value as value1, y.key as key2, y.value as value2 
 FROM src1 x JOIN src y ON (x.key = y.key)) subq
 JOIN srcpart z ON (subq.key1 = z.key and z.ds='2008-04-08' and z.hr=11);

select * from dest_j1 x order by x.key;



