FROM (
 FROM 
  (
  FROM src src1 SELECT src1.key AS c1, src1.value AS c2 WHERE src1.key > 10 and src1.key < 20
  ) a
 LEFT OUTER JOIN 
 (
  FROM src src2 SELECT src2.key AS c3, src2.value AS c4 WHERE src2.key > 15 and src2.key < 25
 ) b 
 ON (a.c1 = b.c3)
 SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
) c
SELECT c.c1, c.c2, c.c3, c.c4 where c.c3 IS NULL AND c.c1 IS NOT NULL

