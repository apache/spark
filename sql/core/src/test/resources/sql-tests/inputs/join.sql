-- self-join (auto_join0.q)
SELECT a.k1, a.v1, a.k2, a.v2
FROM (
SELECT src1.key as k1, src1.value as v1,
       src2.key as k2, src2.value as v2 FROM
  (SELECT * FROM src WHERE src.key < 200) src1
    JOIN
  (SELECT * FROM src WHERE src.key < 200) src2
  SORT BY k1, v1, k2, v2
) a;

-- self-join (auto_join1.q)
SELECT src1.key, src2.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key);

-- self-join (auto_join2.q)
SELECT src1.key, src3.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
  JOIN src src3 ON (src1.key + src2.key = src3.key);

-- self-join (auto_join3.q)
SELECT src1.key, src3.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key = src3.key);

-- left-outer join (auto_join4.q)
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
 SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
 FROM
 (
   SELECT src1.key AS c1, src1.value AS c2 FROM src src1 WHERE src1.key > 100 and src1.key < 300
 ) a
 LEFT OUTER JOIN
 (
   SELECT src2.key AS c3, src2.value AS c4 FROM src src2 WHERE src2.key > 200 and src2.key < 400
 ) b
 ON (a.c1 = b.c3)
) c;

-- right-outer join (auto_join5.q)
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
 SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
 FROM
 (
   SELECT src1.key AS c1, src1.value AS c2 FROM src src1 WHERE src1.key > 100 and src1.key < 300
 ) a
 RIGHT OUTER JOIN
 (
   SELECT src2.key AS c3, src2.value AS c4 FROM src src2 WHERE src2.key > 200 and src2.key < 400
 ) b
 ON (a.c1 = b.c3)
) c;

-- full-outer join (auto_join6.q)
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
 SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
 FROM
 (
   SELECT src1.key AS c1, src1.value AS c2 FROM src src1 WHERE src1.key > 100 and src1.key < 300
 ) a
 FULL OUTER JOIN
 (
   SELECT src2.key AS c3, src2.value AS c4 FROM src src2 WHERE src2.key > 200 and src2.key < 400
 ) b
 ON (a.c1 = b.c3)
) c;

-- full-outer join + left-outer join (auto_join7.q)
SELECT c.c1, c.c2, c.c3, c.c4, c.c5, c.c6
FROM (
  SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4, c.c5 AS c5, c.c6 AS c6
  FROM
  (
    SELECT src1.key AS c1, src1.value AS c2 FROM src src1 WHERE src1.key > 10 and src1.key < 150
  ) a
  FULL OUTER JOIN
  (
    SELECT src2.key AS c3, src2.value AS c4 FROM src src2 WHERE src2.key > 150 and src2.key < 300
  ) b
  ON (a.c1 = b.c3)
  LEFT OUTER JOIN
  (
    SELECT src3.key AS c5, src3.value AS c6 FROM src src3 WHERE src3.key > 200 and src3.key < 400
  ) c
  ON (a.c1 = c.c5)
) c;

-- left-outer join (auto_join8.q)
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
 SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
 FROM
 (
   SELECT src1.key AS c1, src1.value AS c2 FROM src src1 WHERE src1.key > 100 and src1.key < 300
 ) a
 LEFT OUTER JOIN
 (
   SELECT src2.key AS c3, src2.value AS c4 FROM src src2 WHERE src2.key > 200 and src2.key < 400
 ) b
 ON (a.c1 = b.c3)
) c
where c.c3 IS NULL AND c.c1 IS NOT NULL;

-- join (auto_join9.q)
SELECT src1.key, src2.value
FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
WHERE src1.ds = '2008-04-08' and src1.hr = '12'
