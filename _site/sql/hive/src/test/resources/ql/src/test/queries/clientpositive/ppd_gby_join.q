set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
SELECT src1.c1, count(1) 
FROM
(SELECT src.key AS c1, src.value AS c2 from src where src.key > '1' ) src1
JOIN
(SELECT src.key AS c3, src.value AS c4 from src where src.key > '2' ) src2
ON src1.c1 = src2.c3 AND src1.c1 < '400'
WHERE src1.c1 > '20' AND (src1.c2 < 'val_50' OR src1.c1 > '2') AND (src2.c3 > '50' OR src1.c1 < '50') AND (src2.c3 <> '4')
GROUP BY src1.c1;

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
SELECT src1.c1, count(1) 
FROM
(SELECT src.key AS c1, src.value AS c2 from src where src.key > '1' ) src1
JOIN
(SELECT src.key AS c3, src.value AS c4 from src where src.key > '2' ) src2
ON src1.c1 = src2.c3 AND src1.c1 < '400'
WHERE src1.c1 > '20' AND (src1.c2 < 'val_50' OR src1.c1 > '2') AND (src2.c3 > '50' OR src1.c1 < '50') AND (src2.c3 <> '4')
GROUP BY src1.c1;
