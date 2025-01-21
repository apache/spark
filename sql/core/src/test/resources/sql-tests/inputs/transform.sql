-- Test data.
CREATE OR REPLACE TEMPORARY VIEW t AS SELECT * FROM VALUES
('1', true, unhex('537061726B2053514C'), tinyint(1), 1, smallint(100), bigint(1), float(1.0), 1.0, Decimal(1.0), timestamp('1997-01-02'), date('2000-04-01')),
('2', false, unhex('537061726B2053514C'), tinyint(2), 2,  smallint(200), bigint(2), float(2.0), 2.0, Decimal(2.0), timestamp('1997-01-02 03:04:05'), date('2000-04-02')),
('3', true, unhex('537061726B2053514C'), tinyint(3), 3, smallint(300), bigint(3), float(3.0), 3.0, Decimal(3.0), timestamp('1997-02-10 17:32:01-08'), date('2000-04-03'))
AS t(a, b, c, d, e, f, g, h, i, j, k, l);

CREATE OR REPLACE TEMPORARY VIEW script_trans AS SELECT * FROM VALUES
(1, 2, 3),
(4, 5, 6),
(7, 8, 9)
AS script_trans(a, b, c);

CREATE OR REPLACE TEMPORARY VIEW complex_trans AS SELECT * FROM VALUES
(1, 1),
(1, 1),
(2, 2),
(2, 2),
(3, 3),
(2, 2),
(3, 3),
(1, 1),
(3, 3)
as complex_trans(a, b);

SELECT TRANSFORM(a)
USING 'cat' AS (a)
FROM t;

-- common supported data types between no serde and serde transform
SELECT a, b, decode(c, 'UTF-8'), d, e, f, g, h, i, j, k, l FROM (
  SELECT TRANSFORM(a, b, c, d, e, f, g, h, i, j, k, l)
  USING 'cat' AS (
    a string,
    b boolean,
    c binary,
    d tinyint,
    e int,
    f smallint,
    g long,
    h float,
    i double,
    j decimal(38, 18),
    k timestamp,
    l date)
  FROM t
) tmp;

-- common supported data types between no serde and serde transform
SELECT a, b, decode(c, 'UTF-8'), d, e, f, g, h, i, j, k, l FROM (
  SELECT TRANSFORM(a, b, c, d, e, f, g, h, i, j, k, l)
  USING 'cat' AS (
    a string,
    b string,
    c string,
    d string,
    e string,
    f string,
    g string,
    h string,
    i string,
    j string,
    k string,
    l string)
  FROM t
) tmp;

-- SPARK-32388 handle schema less
SELECT TRANSFORM(a)
USING 'cat'
FROM t;

SELECT TRANSFORM(a, b)
USING 'cat'
FROM t;

SELECT TRANSFORM(a, b, c)
USING 'cat'
FROM t;

-- return null when return string incompatible (no serde)
SELECT TRANSFORM(a, b, c, d, e, f, g, h, i)
USING 'cat' AS (a int, b short, c long, d byte, e float, f double, g decimal(38, 18), h date, i timestamp)
FROM VALUES
('a','','1231a','a','213.21a','213.21a','0a.21d','2000-04-01123','1997-0102 00:00:') tmp(a, b, c, d, e, f, g, h, i);

-- SPARK-28227: transform can't run with aggregation
SELECT TRANSFORM(b, max(a), sum(f))
USING 'cat' AS (a, b)
FROM t
GROUP BY b;

-- transform use MAP
MAP a, b USING 'cat' AS (a, b) FROM t;

-- transform use REDUCE
REDUCE a, b USING 'cat' AS (a, b) FROM t;

-- transform with defined row format delimit
SELECT TRANSFORM(a, b, c, null)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
  NULL DEFINED AS 'NULL'
USING 'cat' AS (a, b, c, d)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
  NULL DEFINED AS 'NULL'
FROM t;

SELECT TRANSFORM(a, b, c, null)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
  NULL DEFINED AS 'NULL'
USING 'cat' AS (d)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
  NULL DEFINED AS 'NULL'
FROM t;

SELECT TRANSFORM(a, b, c, null)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
USING 'cat' AS (a, b, c, d)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
  NULL DEFINED AS 'NULL'
FROM t;

SELECT TRANSFORM(a, b, c, null)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
USING 'cat' AS (a, b, c, d)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
FROM t;

SELECT TRANSFORM(a, b, c, null)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
  NULL DEFINED AS 'XXXX'
USING 'cat' AS (a, b, c, d)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
FROM t;

SELECT TRANSFORM(a, b, c, null)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
  NULL DEFINED AS '\n'
USING 'cat' AS (a, b, c, d)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '@'
  LINES TERMINATED BY '\n'
FROM t;

-- transform with defined row format delimit handle schema with correct type
SELECT a, b, decode(c, 'UTF-8'), d, e, f, g, h, i, j, k, l FROM (
  SELECT TRANSFORM(a, b, c, d, e, f, g, h, i, j, k, l)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    NULL DEFINED AS 'NULL'
    USING 'cat' AS (
      a string,
      b boolean,
      c binary,
      d tinyint,
      e int,
      f smallint,
      g long,
      h float,
      i double,
      j decimal(38, 18),
      k timestamp,
      l date)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    NULL DEFINED AS 'NULL'
  FROM t
) tmp;

-- transform with defined row format delimit handle schema with wrong type
SELECT a, b, decode(c, 'UTF-8'), d, e, f, g, h, i, j, k, l FROM (
  SELECT TRANSFORM(a, b, c, d, e, f, g, h, i, j, k, l)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    NULL DEFINED AS 'NULL'
    USING 'cat' AS (
      a string,
      b long,
      c binary,
      d tinyint,
      e int,
      f smallint,
      g long,
      h float,
      i double,
      j decimal(38, 18),
      k int,
      l long)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    NULL DEFINED AS 'NULL'
  FROM t
) tmp;

-- transform with defined row format delimit LINE TERMINATED BY only support '\n'
SELECT a, b, decode(c, 'UTF-8'), d, e, f, g, h, i, j, k, l FROM (
  SELECT TRANSFORM(a, b, c, d, e, f, g, h, i, j, k, l)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '@'
    NULL DEFINED AS 'NULL'
    USING 'cat' AS (
      a string,
      b string,
      c string,
      d string,
      e string,
      f string,
      g string,
      h string,
      i string,
      j string,
      k string,
      l string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '@'
    NULL DEFINED AS 'NULL'
  FROM t
) tmp;

SELECT TRANSFORM(b, a, CAST(c AS STRING))
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 4;

SELECT TRANSFORM(1, 2, 3)
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 4;

SELECT TRANSFORM(1, 2)
  USING 'cat' AS (a INT, b INT)
FROM script_trans
LIMIT 1;

SELECT TRANSFORM(
  b, a,
  CASE
    WHEN c > 100 THEN 1
    WHEN c < 100 THEN 2
  ELSE 3 END)
  USING 'cat' AS (a, b,  c)
FROM script_trans
WHERE a <= 4;

SELECT TRANSFORM(b, a, c + 1)
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 4;

SELECT TRANSFORM(*)
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 4;

SELECT TRANSFORM(b, MAX(a), CAST(SUM(c) AS STRING))
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 4
GROUP BY b;

SELECT TRANSFORM(b, MAX(a) FILTER (WHERE a > 3), CAST(SUM(c) AS STRING))
  USING 'cat' AS (a,b,c)
FROM script_trans
WHERE a <= 4
GROUP BY b;

SELECT TRANSFORM(b, MAX(a), CAST(sum(c) AS STRING))
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 2
GROUP BY b;

SELECT TRANSFORM(b, MAX(a), CAST(SUM(c) AS STRING))
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 4
GROUP BY b
HAVING MAX(a) > 0;

SELECT TRANSFORM(b, MAX(a), CAST(SUM(c) AS STRING))
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 4
GROUP BY b
HAVING MAX(a) > 1;

SELECT TRANSFORM(b, MAX(a) OVER w, CAST(SUM(c) OVER w AS STRING))
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 4
WINDOW w AS (PARTITION BY b ORDER BY a);

SELECT TRANSFORM(b, MAX(a), CAST(SUM(c) AS STRING), myCol, myCol2)
  USING 'cat' AS (a STRING, b STRING, c STRING, d ARRAY<INT>, e STRING)
FROM script_trans
LATERAL VIEW explode(array(array(1,2,3))) myTable AS myCol
LATERAL VIEW explode(myTable.myCol) myTable2 AS myCol2
WHERE a <= 4
GROUP BY b, myCol, myCol2
HAVING max(a) > 1;

FROM(
  FROM script_trans
  SELECT TRANSFORM(a, b)
    USING 'cat' AS (`a` INT, b STRING)
) t
SELECT a + 1;

FROM(
  SELECT TRANSFORM(a, SUM(b))
    USING 'cat' AS (`a` INT, b STRING)
  FROM script_trans
  GROUP BY a
) t
SELECT (b + 1) AS result
ORDER BY result;

MAP k / 10 USING 'cat' AS (one) FROM (SELECT 10 AS k);

FROM (SELECT 1 AS key, 100 AS value) src
MAP src.*, src.key, CAST(src.key / 10 AS INT), CAST(src.key % 10 AS INT), src.value
  USING 'cat' AS (k, v, tkey, ten, one, tvalue);

SELECT TRANSFORM(1)
  USING 'cat' AS (a)
FROM script_trans
HAVING true;

SET spark.sql.legacy.parser.havingWithoutGroupByAsWhere=true;

SELECT TRANSFORM(1)
  USING 'cat' AS (a)
FROM script_trans
HAVING true;

SET spark.sql.legacy.parser.havingWithoutGroupByAsWhere=false;

-- SPARK-34634: self join using CTE contains transform
WITH temp AS (
  SELECT TRANSFORM(a) USING 'cat' AS (b string) FROM t
)
SELECT t1.b FROM temp t1 JOIN temp t2 ON t1.b = t2.b;

SELECT TRANSFORM(DISTINCT b, a, c)
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 4;

SELECT TRANSFORM(ALL b, a, c)
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 4;

-- SPARK-35070: TRANSFORM not support alias in inputs
SELECT TRANSFORM(b AS b_1, MAX(a), CAST(sum(c) AS STRING))
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 2
GROUP BY b;

SELECT TRANSFORM(b b_1, MAX(a), CAST(sum(c) AS STRING))
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 2
GROUP BY b;

SELECT TRANSFORM(b, MAX(a) AS max_a, CAST(sum(c) AS STRING))
  USING 'cat' AS (a, b, c)
FROM script_trans
WHERE a <= 2
GROUP BY b;

-- SPARK-33985: TRANSFORM with CLUSTER BY/ORDER BY/SORT BY
FROM (
  SELECT TRANSFORM(a, b)
    USING 'cat' AS (a, b)
  FROM complex_trans
  CLUSTER BY a
) map_output
SELECT TRANSFORM(a, b)
  USING 'cat' AS (a, b);

FROM (
  SELECT TRANSFORM(a, b)
    USING 'cat' AS (a, b)
  FROM complex_trans
  ORDER BY a
) map_output
SELECT TRANSFORM(a, b)
  USING 'cat' AS (a, b);

SELECT TRANSFORM (a, b)
  USING 'cat' AS (a CHAR(10), b VARCHAR(10))
FROM VALUES('apache', 'spark') t(a, b);
