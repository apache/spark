set hive.groupby.orderby.position.alias=true;

CREATE TABLE testTable1(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE testTable2(key INT, val1 STRING, val2 STRING) STORED AS TEXTFILE;

-- Position Alias in GROUP BY and ORDER BY

EXPLAIN
FROM SRC
INSERT OVERWRITE TABLE testTable1 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) WHERE SRC.key < 20 GROUP BY 1 
INSERT OVERWRITE TABLE testTable2 SELECT SRC.key, SRC.value, COUNT(DISTINCT SUBSTR(SRC.value,5)) WHERE SRC.key < 20 GROUP BY 1, 2;

FROM SRC
INSERT OVERWRITE TABLE testTable1 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) WHERE SRC.key < 20 GROUP BY 1
INSERT OVERWRITE TABLE testTable2 SELECT SRC.key, SRC.value, COUNT(DISTINCT SUBSTR(SRC.value,5)) WHERE SRC.key < 20 GROUP BY 1, 2;

SELECT key, value FROM testTable1 ORDER BY 1, 2;
SELECT key, val1, val2 FROM testTable2 ORDER BY 1, 2, 3;

EXPLAIN
FROM SRC
INSERT OVERWRITE TABLE testTable1 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) WHERE SRC.key < 20 GROUP BY 1
INSERT OVERWRITE TABLE testTable2 SELECT SRC.key, SRC.value, COUNT(DISTINCT SUBSTR(SRC.value,5)) WHERE SRC.key < 20 GROUP BY 2, 1;

FROM SRC
INSERT OVERWRITE TABLE testTable1 SELECT SRC.key, COUNT(DISTINCT SUBSTR(SRC.value,5)) WHERE SRC.key < 20 GROUP BY 1
INSERT OVERWRITE TABLE testTable2 SELECT SRC.key, SRC.value, COUNT(DISTINCT SUBSTR(SRC.value,5)) WHERE SRC.key < 20 GROUP BY 2, 1;

SELECT key, value FROM testTable1 ORDER BY 1, 2;
SELECT key, val1, val2 FROM testTable2 ORDER BY 1, 2, 3;

-- Position Alias in subquery

EXPLAIN
SELECT t.key, t.value
FROM (SELECT b.key as key, count(1) as value FROM src b WHERE b.key <= 20 GROUP BY 1) t
ORDER BY 2 DESC, 1 ASC;

SELECT t.key, t.value
FROM (SELECT b.key as key, count(1) as value FROM src b WHERE b.key <= 20 GROUP BY 1) t
ORDER BY 2 DESC, 1 ASC;

EXPLAIN
SELECT c1, c2, c3, c4
FROM (
 FROM 
  (
  FROM src src1 SELECT src1.key AS c1, src1.value AS c2, COUNT(DISTINCT SUBSTR(src1.value,5)) AS c3 WHERE src1.key > 10 and src1.key < 20 GROUP BY 1, 2
  ) a
 JOIN 
 (
  FROM src src2 SELECT src2.key AS c3, src2.value AS c4 WHERE src2.key > 15 and src2.key < 25 GROUP BY 1, 2
 ) b 
 ON (a.c1 = b.c3)
 SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
) c
ORDER BY 1 DESC, 2 DESC, 3 ASC, 4 ASC;

SELECT c1, c2, c3, c4
FROM (
 FROM 
  (
  FROM src src1 SELECT src1.key AS c1, src1.value AS c2, COUNT(DISTINCT SUBSTR(src1.value,5)) AS c3 WHERE src1.key > 10 and src1.key < 20 GROUP BY 1, 2
  ) a
 JOIN 
 (
  FROM src src2 SELECT src2.key AS c3, src2.value AS c4 WHERE src2.key > 15 and src2.key < 25 GROUP BY 1, 2
 ) b 
 ON (a.c1 = b.c3)
 SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
) c
ORDER BY 1 DESC, 2 DESC, 3 ASC, 4 ASC;
