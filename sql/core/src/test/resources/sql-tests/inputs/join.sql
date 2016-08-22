-- join nested table expressions
SELECT a.k1, a.v1, a.k2, a.v2
FROM (
SELECT src1.key as k1, src1.value as v1,
       src2.key as k2, src2.value as v2 FROM
  (SELECT * FROM duplicateRowData WHERE duplicateRowData.key < 200) src1
    JOIN
  (SELECT * FROM duplicateRowData WHERE duplicateRowData.key < 200) src2
  SORT BY k1, v1, k2, v2
) a;

-- self-join + join condition
SELECT src1.key, src2.value
FROM duplicateRowData src1 JOIN duplicateRowData src2 ON (src1.key = src2.key);

-- equi inner join + inner join with a complex join condition
SELECT src1.key, src3.value
FROM duplicateRowData src1 JOIN duplicateRowData src2 ON (src1.key = src2.key)
  JOIN duplicateRowData src3 ON (src1.key + src2.key = src3.key);

-- equi inner join + equi inner join
SELECT src1.key, src3.value
FROM duplicateRowData src1 JOIN duplicateRowData src2 ON (src1.key = src2.key)
  JOIN duplicateRowData src3 ON (src1.key = src3.key);

-- inner join + join condition + filter
SELECT src1.key, src2.value
FROM partitionedData src1 JOIN duplicateRowData src2 ON (src1.key = src2.key)
WHERE src1.ds = '2008-04-08' and src1.hr = '12';

-- equi inner join + table.star expansion in nested table expression
FROM
(SELECT duplicateRowData.* from duplicateRowData) x
JOIN
(SELECT duplicateRowData.* from duplicateRowData) Y
ON (x.key = Y.key)
select Y.key, Y.value;

-- inner join with a complex join condition over nested table expressions
SELECT src1.c1, src2.c4
FROM
  (SELECT duplicateRowData.key as c1, duplicateRowData.value as c2 from duplicateRowData) src1
  JOIN
  (SELECT duplicateRowData.key as c3, duplicateRowData.value as c4 from duplicateRowData) src2
  ON src1.c1 = src2.c3 AND src1.c1 < 200;

-- two inner join with a complex join condition over nested table expressions
SELECT src1.c1, src2.c4
FROM
  (SELECT duplicateRowData.key as c1, duplicateRowData.value as c2 from duplicateRowData) src1
  JOIN
  (SELECT duplicateRowData.key as c3, duplicateRowData.value as c4 from duplicateRowData) src2
  ON src1.c1 = src2.c3 AND src1.c1 < 200
  JOIN
  (SELECT duplicateRowData.key as c5, duplicateRowData.value as c6 from duplicateRowData) src3
  ON src1.c1 = src3.c5 AND src3.c5 < 100;

-- two inner join with a complex join condition over nested table expressions
SELECT src1.c1, src2.c4
FROM
  (SELECT duplicateRowData.key as c1, duplicateRowData.value as c2 from duplicateRowData) src1
  JOIN
  (SELECT duplicateRowData.key as c3, duplicateRowData.value as c4 from duplicateRowData) src2
  ON src1.c1 = src2.c3 AND src1.c1 < 250
  JOIN
  (SELECT duplicateRowData.key as c5, duplicateRowData.value as c6 from duplicateRowData) src3
  ON src1.c1 + src2.c3 = src3.c5 AND src3.c5 < 400;

-- join two different tables
FROM duplicateRowData JOIN partitionedData
ON duplicateRowData.key = partitionedData.key AND partitionedData.ds = '2008-04-08'
  AND duplicateRowData.key > 200
SELECT duplicateRowData.key, partitionedData.value;

-- join + sort by
SELECT a.k1, a.v1, a.k2, a.v2
  FROM (
  SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2
  FROM duplicateRowData src1 JOIN duplicateRowData src2 ON (src1.key = src2.key)
  SORT BY k1, v1, k2, v2
  ) a;

-- inner join with a filter above join and a filter below join
SELECT subq.key, tab.value
FROM
(select a.key, a.value from duplicateRowData a where a.key > 100 ) subq
JOIN duplicateRowData tab
ON (subq.key = tab.key and subq.key > 150 and subq.value = tab.value)
where tab.key < 200;

-- star expansion in nested table expression
SELECT src1.*, src2.*
FROM duplicateRowData src1 JOIN duplicateRowData src2 ON (src1.key = src2.key);

-- join + disjunctive conditions
SELECT src1.key, src2.value
FROM partitionedData src1 JOIN duplicateRowData src2 ON (src1.key = src2.key)
where (src1.ds = '2008-04-08' or src1.ds = '2008-04-09' )and (src1.hr = '12' or src1.hr = '11');

-- nested join
SELECT src5.src1_value
FROM
  (SELECT src3.*, src4.value as src4_value, src4.key as src4_key
  FROM duplicateRowData src4
    JOIN (SELECT src2.*, src1.key as src1_key, src1.value as src1_value
      FROM duplicateRowData src1
        JOIN duplicateRowData src2 ON src1.key = src2.key) src3
    ON src3.src1_key = src4.key) src5;

-- Cartesian join
SELECT  *  FROM duplicateRowData src1 JOIN duplicateRowData src2
WHERE src1.key < 200 and src2.key < 200
SORT BY src1.key, src1.value, src2.key, src2.value;

-- join
WITH tst1 AS (SELECT a.key, count(1) as cnt FROM duplicateRowData a group by a.key)
SELECT sum(a.cnt) FROM tst1 a JOIN tst1 b ON a.key = b.key;

-- aggregate over join results
SELECT x.key, count(1)
FROM nullData x JOIN duplicateRowData y
ON (x.key = y.key) group by x.key order by x.key;

-- join over set operation over aggregate
SELECT count(1)
FROM
(
  SELECT duplicateRowData.key, duplicateRowData.value from duplicateRowData
  UNION ALL
  SELECT DISTINCT duplicateRowData.key, duplicateRowData.value from duplicateRowData
) src_12
JOIN
(
  SELECT duplicateRowData.key as k, duplicateRowData.value as v from duplicateRowData
) src3
ON src_12.key = src3.k AND src3.k < 300;

-- inner join with sorted by nested table expression
FROM
(SELECT duplicateRowData.* FROM duplicateRowData sort by key) x
JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Y
ON (x.key = Y.key)
select Y.key,Y.value;

-- inner + inner with sorted by nested table expression
FROM
(SELECT duplicateRowData.* FROM duplicateRowData sort by key) x
JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Y
ON (x.key = Y.key)
JOIN
(SELECT duplicateRowData.* FROM duplicateRowData sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;

-- join over set operation
SELECT x.key, x.value, subq1.value
FROM
( SELECT x.key as key, x.value as value from duplicateRowData x where x.key < 200
     UNION ALL
  SELECT x1.key as key, x1.value as value from duplicateRowData x1 where x1.key > 100
) subq1
JOIN nullData x ON (x.key = subq1.key);

-- join over set operation over aggregate
SELECT x.key, x.value, subq1.cnt
FROM
( SELECT x.key as key, count(1) as cnt from duplicateRowData x where x.key < 200 group by x.key
     UNION ALL
  SELECT x1.key as key, count(1) as cnt from duplicateRowData x1 where x1.key > 100 group by x1.key
) subq1
JOIN nullData x ON (x.key = subq1.key);

-- self join with aliases
SELECT x.key, COUNT(*)
FROM duplicateRowData x JOIN duplicateRowData y ON x.key = y.key
GROUP BY x.key;

-- inner join with one-match-per-row filtering predicates (where)
SELECT * FROM uppercasedata u JOIN lowercasedata l WHERE u.n = l.N;

-- inner join with one-match-per-row join conditions (on)
SELECT * FROM uppercasedata u JOIN lowercasedata l ON u.n = l.N;

-- inner join with multiple-match-per-row filtering predicates (where)
SELECT * FROM
  (SELECT * FROM duplicateColumnValueData WHERE a = 1) x JOIN
  (SELECT * FROM duplicateColumnValueData WHERE a = 1) y
WHERE x.a = y.a;

-- inner join with no-match-per-row filtering predicates (where)
SELECT * FROM
  (SELECT * FROM duplicateColumnValueData WHERE a = 1) x JOIN
  (SELECT * FROM duplicateColumnValueData WHERE a = 2) y
WHERE x.a = y.a;

-- inner join ON with table name as qualifier
SELECT * FROM upperCaseData JOIN lowerCaseData ON lowerCaseData.n = upperCaseData.N;

-- qualified select with inner join ON with table name as qualifier
SELECT upperCaseData.N, upperCaseData.L FROM upperCaseData JOIN lowerCaseData
  ON lowerCaseData.n = upperCaseData.N;

-- SPARK-4120 Join of multiple tables does not work in SparkSQL
SELECT a.key, b.key, c.key
FROM uniqueRowData a,uniqueRowData b,uniqueRowData c
where a.key = b.key and a.key = c.key and a.key < 5;

-- big inner join, 4 matches per row
SELECT x.key, x.value, y.key, y.value, count(1) FROM
  (SELECT * FROM uniqueRowData UNION ALL
   SELECT * FROM uniqueRowData UNION ALL
   SELECT * FROM uniqueRowData UNION ALL
   SELECT * FROM uniqueRowData) x JOIN
  (SELECT * FROM uniqueRowData UNION ALL
   SELECT * FROM uniqueRowData UNION ALL
   SELECT * FROM uniqueRowData UNION ALL
   SELECT * FROM uniqueRowData) y
WHERE x.key = y.key group by x.key, x.value, y.key, y.value;

-- mixed-case keywords
SeleCT * from
  (select * from upperCaseData WherE N <= 4) leftTable fuLL OUtER joiN
  (sElEcT * FROM upperCaseData whERe N >= 3) rightTable
    oN leftTable.N = rightTable.N;

-- Supporting relational operator '<=>' in Spark SQL
SELECT * FROM nullData as a JOIN nullData as b on a.value <=> b.value;
