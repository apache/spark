-- basic full outer join
SELECT * FROM
  (SELECT * FROM upperCaseData WHERE N <= 4) leftTable FULL OUTER JOIN
  (SELECT * FROM upperCaseData WHERE N >= 3) rightTable
    ON leftTable.N = rightTable.N;

-- basic right outer join
SELECT * FROM lowercasedata l RIGHT OUTER JOIN uppercasedata u ON l.n = u.N;

-- basic left outer join
SELECT * FROM uppercasedata u LEFT OUTER JOIN lowercasedata l ON l.n = u.N;

-- left-outer join over two nested table expressions
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
  SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
  FROM
  (
    SELECT src1.key AS c1, src1.value AS c2
    FROM duplicateRowData src1 WHERE src1.key > 100 and src1.key < 300
  ) a
  LEFT OUTER JOIN
  (
    SELECT src2.key AS c3, src2.value AS c4
    FROM duplicateRowData src2 WHERE src2.key > 200 and src2.key < 400
  ) b
  ON (a.c1 = b.c3)
) c;

-- right-outer join over two nested table expressions
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
  SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
  FROM
  (
    SELECT src1.key AS c1, src1.value AS c2
    FROM duplicateRowData src1 WHERE src1.key > 100 and src1.key < 300
  ) a
  RIGHT OUTER JOIN
  (
    SELECT src2.key AS c3, src2.value AS c4
    FROM duplicateRowData src2 WHERE src2.key > 200 and src2.key < 400
  ) b
  ON (a.c1 = b.c3)
) c;

-- full-outer join over two nested table expressions
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
 SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
 FROM
 (
   SELECT src1.key AS c1, src1.value AS c2
   FROM duplicateRowData src1 WHERE src1.key > 100 and src1.key < 300
 ) a
 FULL OUTER JOIN
 (
   SELECT src2.key AS c3, src2.value AS c4
   FROM duplicateRowData src2 WHERE src2.key > 200 and src2.key < 400
 ) b
 ON (a.c1 = b.c3)
) c;

-- full-outer join + left-outer join over nested table expressions
SELECT c.c1, c.c2, c.c3, c.c4, c.c5, c.c6
FROM (
  SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4, c.c5 AS c5, c.c6 AS c6
  FROM
  (
    SELECT src1.key AS c1, src1.value AS c2
    FROM duplicateRowData src1 WHERE src1.key > 10 and src1.key < 150
  ) a
  FULL OUTER JOIN
  (
    SELECT src2.key AS c3, src2.value AS c4
    FROM duplicateRowData src2 WHERE src2.key > 150 and src2.key < 300
  ) b
  ON (a.c1 = b.c3)
  LEFT OUTER JOIN
  (
    SELECT src3.key AS c5, src3.value AS c6
    FROM duplicateRowData src3 WHERE src3.key > 200 and src3.key < 400
  ) c
  ON (a.c1 = c.c5)
) c;

-- left-outer join + join condition + filter
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
 SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
 FROM
 (
   SELECT src1.key AS c1, src1.value AS c2
   FROM duplicateRowData src1 WHERE src1.key > 100 and src1.key < 300
 ) a
 LEFT OUTER JOIN
 (
   SELECT src2.key AS c3, src2.value AS c4
   FROM duplicateRowData src2 WHERE src2.key > 200 and src2.key < 400
 ) b
 ON (a.c1 = b.c3)
) c
where c.c3 IS NULL AND c.c1 IS NOT NULL;

-- full outer join over Aggregate
SELECT a.key, a.value, b.key, b.value
FROM
  (
    SELECT src1.key as key, count(src1.value) AS value
    FROM duplicateRowData src1 group by src1.key
  ) a
  FULL OUTER JOIN
  (
    SELECT src2.key as key, count(distinct(src2.value)) AS value
    FROM nullData src2 group by src2.key
  ) b
ON (a.key = b.key);

-- full outer join + multi distinct
SELECT a.key, a.value, b.key, b.value1, b.value2
FROM
  (
    SELECT src1.key as key, count(src1.value) AS value
    FROM duplicateRowData src1 group by src1.key
  ) a
  FULL OUTER JOIN
  (
    SELECT src2.key as key, count(distinct(src2.value)) AS value1,
      count(distinct(src2.key)) AS value2
    FROM nullData src2 group by src2.key
  ) b
ON (a.key = b.key);

-- inner join + right-outer join #1
SELECT a.k1,a.v1,a.k2,a.v2,a.k3,a.v3
FROM (
  SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2 , src3.key as k3,
    src3.value as v3
  FROM duplicateRowData src1 JOIN duplicateRowData src2 ON (src1.key = src2.key AND src1.key < 200)
    RIGHT OUTER JOIN duplicateRowData src3 ON (src1.key = src3.key AND src3.key < 300)
  SORT BY k1,v1,k2,v2,k3,v3
)a;

-- inner join + right-outer join #2
SELECT a.k1,a.v1,a.k2,a.v2,a.k3,a.v3
FROM (
  SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2 , src3.key as k3,
    src3.value as v3
  FROM duplicateRowData src1
    JOIN duplicateRowData src2
      ON (src1.key = src2.key AND src1.key < 200 AND src2.key < 100)
    RIGHT OUTER JOIN duplicateRowData src3
      ON (src1.key = src3.key AND src3.key < 300)
  SORT BY k1,v1,k2,v2,k3,v3
)a;

-- left outer join + right outer join
SELECT *
FROM
  duplicateRowData src1
  LEFT OUTER JOIN duplicateRowData src2
    ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
  RIGHT OUTER JOIN duplicateRowData src3
    ON (src2.key = src3.key AND src3.key < 200)
SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- left outer + right outer
SELECT * FROM duplicateRowData src1
 LEFT OUTER JOIN duplicateRowData src2
   ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 RIGHT OUTER JOIN duplicateRowData src3
   ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- right outer + right outer
SELECT * FROM duplicateRowData src1
 RIGHT OUTER JOIN duplicateRowData src2
   ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 RIGHT OUTER JOIN duplicateRowData src3
   ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- left outer + left outer
SELECT * FROM duplicateRowData src1
 LEFT OUTER JOIN duplicateRowData src2
   ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 LEFT OUTER JOIN duplicateRowData src3
   ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- right outer + left outer
SELECT * FROM duplicateRowData src1
 RIGHT OUTER JOIN duplicateRowData src2
   ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 LEFT OUTER JOIN duplicateRowData src3
   ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- inner + left outer
SELECT * FROM duplicateRowData src1
 JOIN duplicateRowData src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 LEFT OUTER JOIN duplicateRowData src3 ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- inner + right outer
SELECT * FROM duplicateRowData src1
 JOIN duplicateRowData src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 RIGHT OUTER JOIN duplicateRowData src3 ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- left + inner outer
SELECT * FROM duplicateRowData src1
 LEFT OUTER JOIN duplicateRowData src2
   ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 JOIN duplicateRowData src3
   ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- right + inner join
SELECT * FROM duplicateRowData src1
 RIGHT OUTER JOIN duplicateRowData src2
   ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 JOIN duplicateRowData src3
   ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- left outer join with sorted by nested table expression
FROM
(SELECT duplicateRowData.* FROM duplicateRowData sort by key) x
LEFT OUTER JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Y
ON (x.key = Y.key)
select Y.key,Y.value;

-- right outer join with sorted by nested table expression
FROM
(SELECT duplicateRowData.* FROM duplicateRowData sort by key) x
RIGHT OUTER JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Y
ON (x.key = Y.key)
select Y.key,Y.value;

-- inner + left outer with sorted by nested table expression
FROM
(SELECT duplicateRowData.* FROM duplicateRowData sort by key) x
JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;

-- left + left outer with sorted by nested table expression
FROM
(SELECT duplicateRowData.* FROM duplicateRowData sort by key) x
LEFT OUTER JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;

-- left + right outer with sorted by nested table expression
FROM
(SELECT duplicateRowData.* FROM duplicateRowData sort by key) x
LEFT OUTER JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;

-- right + right outer with sorted by nested table expression
FROM
(SELECT duplicateRowData.* FROM duplicateRowData sort by key) x
RIGHT OUTER JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;

-- right outer + inner with sorted by nested table expression
FROM
(SELECT duplicateRowData.* FROM duplicateRowData sort by key) x
RIGHT OUTER JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Y
ON (x.key = Y.key)
JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;
