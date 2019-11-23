EXPLAIN
SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src) src2
ON src1.c1 = src2.c3 AND src1.c1 < 100
JOIN
(SELECT src.key as c5, src.value as c6 from src) src3
ON src1.c1 + src2.c3 = src3.c5 AND src3.c5 < 200;

SELECT src1.c1, src2.c4 
FROM
(SELECT src.key as c1, src.value as c2 from src) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src) src2
ON src1.c1 = src2.c3 AND src1.c1 < 100
JOIN
(SELECT src.key as c5, src.value as c6 from src) src3
ON src1.c1 + src2.c3 = src3.c5 AND src3.c5 < 200;
