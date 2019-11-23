set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src where src.key <> '11' ) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src where src.key <> '12' ) src2
ON src1.c1 = src2.c3 AND src1.c1 < '400'
JOIN
(SELECT src.key as c5, src.value as c6 from src where src.key <> '13' ) src3
ON src1.c1 = src3.c5
WHERE src1.c1 > '0' and (src1.c2 <> 'val_500' or src1.c1 > '1') and (src2.c3 > '10' or src1.c1 <> '10') and (src2.c3 <> '4') and (src3.c5 <> '1');

SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src where src.key <> '11' ) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src where src.key <> '12' ) src2
ON src1.c1 = src2.c3 AND src1.c1 < '400'
JOIN
(SELECT src.key as c5, src.value as c6 from src where src.key <> '13' ) src3
ON src1.c1 = src3.c5
WHERE src1.c1 > '0' and (src1.c2 <> 'val_500' or src1.c1 > '1') and (src2.c3 > '10' or src1.c1 <> '10') and (src2.c3 <> '4') and (src3.c5 <> '1');

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src where src.key <> '11' ) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src where src.key <> '12' ) src2
ON src1.c1 = src2.c3 AND src1.c1 < '400'
JOIN
(SELECT src.key as c5, src.value as c6 from src where src.key <> '13' ) src3
ON src1.c1 = src3.c5
WHERE src1.c1 > '0' and (src1.c2 <> 'val_500' or src1.c1 > '1') and (src2.c3 > '10' or src1.c1 <> '10') and (src2.c3 <> '4') and (src3.c5 <> '1');

SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src where src.key <> '11' ) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src where src.key <> '12' ) src2
ON src1.c1 = src2.c3 AND src1.c1 < '400'
JOIN
(SELECT src.key as c5, src.value as c6 from src where src.key <> '13' ) src3
ON src1.c1 = src3.c5
WHERE src1.c1 > '0' and (src1.c2 <> 'val_500' or src1.c1 > '1') and (src2.c3 > '10' or src1.c1 <> '10') and (src2.c3 <> '4') and (src3.c5 <> '1');
