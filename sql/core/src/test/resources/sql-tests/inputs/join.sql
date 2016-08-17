-- join nested table expressions (auto_join0.q)
SELECT a.k1, a.v1, a.k2, a.v2
FROM (
SELECT src1.key as k1, src1.value as v1,
       src2.key as k2, src2.value as v2 FROM
  (SELECT * FROM src WHERE src.key < 200) src1
    JOIN
  (SELECT * FROM src WHERE src.key < 200) src2
  SORT BY k1, v1, k2, v2
) a;

-- self-join + join condition (auto_join1.q)
SELECT src1.key, src2.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key);

-- equi inner join + inner join with a complex join condition (auto_join2.q)
SELECT src1.key, src3.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
  JOIN src src3 ON (src1.key + src2.key = src3.key);

-- equi inner join + equi inner join (auto_join3.q)
SELECT src1.key, src3.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
 JOIN src src3 ON (src1.key = src3.key);

-- inner join + join condition + filter (auto_join9.q)
SELECT src1.key, src2.value
FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
WHERE src1.ds = '2008-04-08' and src1.hr = '12';

-- equi inner join + table.star expansion in nested table expression (auto_join10.q)
FROM
(SELECT src.* FROM src) x
JOIN
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
select Y.key, Y.value;

-- inner join with a complex join condition over nested table expressions (auto_join11.q)
SELECT src1.c1, src2.c4
FROM
  (SELECT src.key as c1, src.value as c2 from src) src1
  JOIN
  (SELECT src.key as c3, src.value as c4 from src) src2
  ON src1.c1 = src2.c3 AND src1.c1 < 200;

-- two inner join with a complex join condition over nested table expressions (auto_join12.q)
SELECT src1.c1, src2.c4
FROM
  (SELECT src.key as c1, src.value as c2 from src) src1
  JOIN
  (SELECT src.key as c3, src.value as c4 from src) src2
  ON src1.c1 = src2.c3 AND src1.c1 < 200
  JOIN
  (SELECT src.key as c5, src.value as c6 from src) src3
  ON src1.c1 = src3.c5 AND src3.c5 < 100;

-- two inner join with a complex join condition over nested table expressions (auto_join13.q)
SELECT src1.c1, src2.c4
FROM
  (SELECT src.key as c1, src.value as c2 from src) src1
  JOIN
  (SELECT src.key as c3, src.value as c4 from src) src2
  ON src1.c1 = src2.c3 AND src1.c1 < 250
  JOIN
  (SELECT src.key as c5, src.value as c6 from src) src3
  ON src1.c1 + src2.c3 = src3.c5 AND src3.c5 < 400;

-- join two different tables (auto_join14.q)
FROM src JOIN srcpart ON src.key = srcpart.key AND srcpart.ds = '2008-04-08' and src.key > 200
SELECT src.key, srcpart.value;

-- join + sort by (auto_join15.q)
SELECT a.k1, a.v1, a.k2, a.v2
  FROM (
  SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2
  FROM src src1 JOIN src src2 ON (src1.key = src2.key)
  SORT BY k1, v1, k2, v2
  ) a;

-- inner join with a filter above join and a filter below join (auto_join16.q)
SELECT subq.key, tab.value
FROM
(select a.key, a.value from src a where a.key > 100 ) subq
JOIN src tab
ON (subq.key = tab.key and subq.key > 150 and subq.value = tab.value)
where tab.key < 200;

-- star expansion in nested table expression (auto_join17.q)
SELECT src1.*, src2.*
FROM src src1 JOIN src src2 ON (src1.key = src2.key);

-- join + disjunctive conditions (auto_join19.q)
SELECT src1.key, src2.value
FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
where (src1.ds = '2008-04-08' or src1.ds = '2008-04-09' )and (src1.hr = '12' or src1.hr = '11');

-- nested join (auto_join22.q)
SELECT src5.src1_value
FROM
  (SELECT src3.*, src4.value as src4_value, src4.key as src4_key
  FROM src src4
    JOIN (SELECT src2.*, src1.key as src1_key, src1.value as src1_value
      FROM src src1
        JOIN src src2 ON src1.key = src2.key) src3
    ON src3.src1_key = src4.key) src5;

-- Cartesian join (auto_join23.q)
SELECT  *  FROM src src1 JOIN src src2
WHERE src1.key < 200 and src2.key < 200
SORT BY src1.key, src1.value, src2.key, src2.value;

-- join (auto_join24.q)
WITH tst1 AS (SELECT a.key, count(1) as cnt FROM src a group by a.key)
SELECT sum(a.cnt) FROM tst1 a JOIN tst1 b ON a.key = b.key;

-- aggregate over join results (auto_join26.q)
SELECT x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key order by x.key;

-- join over set operation over aggregate (auto_join27.q)
SELECT count(1)
FROM
(
  SELECT src.key, src.value from src
  UNION ALL
  SELECT DISTINCT src.key, src.value from src
) src_12
JOIN
(
  SELECT src.key as k, src.value as v from src
) src3
ON src_12.key = src3.k AND src3.k < 300;

-- inner join with sorted by nested table expression (auto_join30.q)
FROM
(SELECT src.* FROM src sort by key) x
JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
select Y.key,Y.value;

-- inner + inner with sorted by nested table expression (auto_join30.q)
FROM
(SELECT src.* FROM src sort by key) x
JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;

-- join over set operation (join34.q)
SELECT x.key, x.value, subq1.value
FROM
( SELECT x.key as key, x.value as value from src x where x.key < 200
     UNION ALL
  SELECT x1.key as key, x1.value as value from src x1 where x1.key > 100
) subq1
JOIN src1 x ON (x.key = subq1.key);

-- join over set operation over aggregate (join35.q)
SELECT x.key, x.value, subq1.cnt
FROM
( SELECT x.key as key, count(1) as cnt from src x where x.key < 200 group by x.key
     UNION ALL
  SELECT x1.key as key, count(1) as cnt from src x1 where x1.key > 100 group by x1.key
) subq1
JOIN src1 x ON (x.key = subq1.key);

-- self join with aliases
SELECT x.key, COUNT(*)
FROM src x JOIN src y ON x.key = y.key
GROUP BY x.key;

-- inner join with one-match-per-row filtering predicates (where)
SELECT * FROM uppercasedata u JOIN lowercasedata l WHERE u.n = l.N;

-- inner join with one-match-per-row join conditions (on)
SELECT * FROM uppercasedata u JOIN lowercasedata l ON u.n = l.N;

-- inner join with multiple-match-per-row filtering predicates (where)
SELECT * FROM
  (SELECT * FROM testdata2 WHERE a = 1) x JOIN
  (SELECT * FROM testdata2 WHERE a = 1) y
WHERE x.a = y.a;

-- inner join with no-match-per-row filtering predicates (where)
SELECT * FROM
  (SELECT * FROM testData2 WHERE a = 1) x JOIN
  (SELECT * FROM testData2 WHERE a = 2) y
WHERE x.a = y.a;

-- inner join ON with table name as qualifier
SELECT * FROM upperCaseData JOIN lowerCaseData ON lowerCaseData.n = upperCaseData.N;

-- qualified select with inner join ON with table name as qualifier
SELECT upperCaseData.N, upperCaseData.L FROM upperCaseData JOIN lowerCaseData
  ON lowerCaseData.n = upperCaseData.N;

-- SPARK-4120 Join of multiple tables does not work in SparkSQL
SELECT a.key, b.key, c.key
FROM testData a,testData b,testData c
where a.key = b.key and a.key = c.key and a.key < 5;

-- big inner join, 4 matches per row
SELECT x.key, x.value, y.key, y.value, count(1) FROM
  (SELECT * FROM testData UNION ALL
   SELECT * FROM testData UNION ALL
   SELECT * FROM testData UNION ALL
   SELECT * FROM testData) x JOIN
  (SELECT * FROM testData UNION ALL
   SELECT * FROM testData UNION ALL
   SELECT * FROM testData UNION ALL
   SELECT * FROM testData) y
WHERE x.key = y.key group by x.key, x.value, y.key, y.value;

-- mixed-case keywords
SeleCT * from
  (select * from upperCaseData WherE N <= 4) leftTable fuLL OUtER joiN
  (sElEcT * FROM upperCaseData whERe N >= 3) rightTable
    oN leftTable.N = rightTable.N;

-- Supporting relational operator '<=>' in Spark SQL
SELECT * FROM src1 as a JOIN src1 as b on a.value <=> b.value;
