set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
SELECT src1.c1 
FROM
(SELECT src.value as c1, count(src.key) as c2 from src where src.value > 'val_10' group by src.value) src1
WHERE src1.c1 > 'val_200' and (src1.c2 > 30 or src1.c1 < 'val_400'); 

SELECT src1.c1 
FROM
(SELECT src.value as c1, count(src.key) as c2 from src where src.value > 'val_10' group by src.value) src1
WHERE src1.c1 > 'val_200' and (src1.c2 > 30 or src1.c1 < 'val_400'); 

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
SELECT src1.c1 
FROM
(SELECT src.value as c1, count(src.key) as c2 from src where src.value > 'val_10' group by src.value) src1
WHERE src1.c1 > 'val_200' and (src1.c2 > 30 or src1.c1 < 'val_400'); 

SELECT src1.c1 
FROM
(SELECT src.value as c1, count(src.key) as c2 from src where src.value > 'val_10' group by src.value) src1
WHERE src1.c1 > 'val_200' and (src1.c2 > 30 or src1.c1 < 'val_400'); 
