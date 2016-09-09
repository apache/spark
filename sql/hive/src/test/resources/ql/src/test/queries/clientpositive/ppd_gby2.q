set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
SELECT max(src1.c1), src1.c2 
FROM
(SELECT src.value AS c1, count(src.key) AS c2 FROM src WHERE src.value > 'val_10' GROUP BY src.value) src1
WHERE src1.c1 > 'val_200' AND (src1.c2 > 30 OR src1.c1 < 'val_400')
GROUP BY src1.c2; 

SELECT max(src1.c1), src1.c2 
FROM
(SELECT src.value AS c1, count(src.key) AS c2 FROM src WHERE src.value > 'val_10' GROUP BY src.value) src1
WHERE src1.c1 > 'val_200' AND (src1.c2 > 30 OR src1.c1 < 'val_400')
GROUP BY src1.c2; 

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
SELECT max(src1.c1), src1.c2 
FROM
(SELECT src.value AS c1, count(src.key) AS c2 FROM src WHERE src.value > 'val_10' GROUP BY src.value) src1
WHERE src1.c1 > 'val_200' AND (src1.c2 > 30 OR src1.c1 < 'val_400')
GROUP BY src1.c2; 

SELECT max(src1.c1), src1.c2 
FROM
(SELECT src.value AS c1, count(src.key) AS c2 FROM src WHERE src.value > 'val_10' GROUP BY src.value) src1
WHERE src1.c1 > 'val_200' AND (src1.c2 > 30 OR src1.c1 < 'val_400')
GROUP BY src1.c2; 
