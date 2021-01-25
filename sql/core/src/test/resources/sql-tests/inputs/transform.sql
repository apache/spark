-- Test data.
CREATE OR REPLACE TEMPORARY VIEW t AS SELECT * FROM VALUES
('1', true, unhex('537061726B2053514C'), tinyint(1), 1, smallint(100), bigint(1), float(1.0), 1.0, Decimal(1.0), timestamp('1997-01-02'), date('2000-04-01')),
('2', false, unhex('537061726B2053514C'), tinyint(2), 2,  smallint(200), bigint(2), float(2.0), 2.0, Decimal(2.0), timestamp('1997-01-02 03:04:05'), date('2000-04-02')),
('3', true, unhex('537061726B2053514C'), tinyint(3), 3, smallint(300), bigint(3), float(3.0), 3.0, Decimal(3.0), timestamp('1997-02-10 17:32:01-08'), date('2000-04-03'))
AS t(a, b, c, d, e, f, g, h, i, j, k, l);

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
