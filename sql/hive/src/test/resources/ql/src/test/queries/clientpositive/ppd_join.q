set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src where src.key > '1' ) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src where src.key > '2' ) src2
ON src1.c1 = src2.c3 AND src1.c1 < '400'
WHERE src1.c1 > '20' and (src1.c2 < 'val_50' or src1.c1 > '2') and (src2.c3 > '50' or src1.c1 < '50') and (src2.c3 <> '4');

SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src where src.key > '1' ) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src where src.key > '2' ) src2
ON src1.c1 = src2.c3 AND src1.c1 < '400'
WHERE src1.c1 > '20' and (src1.c2 < 'val_50' or src1.c1 > '2') and (src2.c3 > '50' or src1.c1 < '50') and (src2.c3 <> '4');

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src where src.key > '1' ) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src where src.key > '2' ) src2
ON src1.c1 = src2.c3 AND src1.c1 < '400'
WHERE src1.c1 > '20' and (src1.c2 < 'val_50' or src1.c1 > '2') and (src2.c3 > '50' or src1.c1 < '50') and (src2.c3 <> '4');

SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src where src.key > '1' ) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src where src.key > '2' ) src2
ON src1.c1 = src2.c3 AND src1.c1 < '400'
WHERE src1.c1 > '20' and (src1.c2 < 'val_50' or src1.c1 > '2') and (src2.c3 > '50' or src1.c1 < '50') and (src2.c3 <> '4');
