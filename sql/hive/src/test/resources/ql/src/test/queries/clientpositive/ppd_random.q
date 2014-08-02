set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src ) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src where src.key > '2' ) src2
ON src1.c1 = src2.c3
WHERE rand() > 0.5;

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src ) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src where src.key > '2' ) src2
ON src1.c1 = src2.c3
WHERE rand() > 0.5;
