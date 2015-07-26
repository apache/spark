CREATE TABLE orc_pred(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           dec decimal(4,2),
           bin binary)
STORED AS ORC;

ALTER TABLE orc_pred SET SERDEPROPERTIES ('orc.row.index.stride' = '1000');

CREATE TABLE staging(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           dec decimal(4,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE staging;

INSERT INTO TABLE orc_pred select * from staging;

-- no predicate case. the explain plan should not have filter expression in table scan operator

SELECT SUM(HASH(t)) FROM orc_pred;

SET hive.optimize.index.filter=true;
SELECT SUM(HASH(t)) FROM orc_pred;
SET hive.optimize.index.filter=false;

EXPLAIN SELECT SUM(HASH(t)) FROM orc_pred;

SET hive.optimize.index.filter=true;
EXPLAIN SELECT SUM(HASH(t)) FROM orc_pred;
SET hive.optimize.index.filter=false;

-- all the following queries have predicates which are pushed down to table scan operator if
-- hive.optimize.index.filter is set to true. the explain plan should show filter expression
-- in table scan operator.

SELECT * FROM orc_pred WHERE t<2 limit 1;
SET hive.optimize.index.filter=true;
SELECT * FROM orc_pred WHERE t<2 limit 1;
SET hive.optimize.index.filter=false;

SELECT * FROM orc_pred WHERE t>2 limit 1;
SET hive.optimize.index.filter=true;
SELECT * FROM orc_pred WHERE t>2 limit 1;
SET hive.optimize.index.filter=false;

SELECT SUM(HASH(t)) FROM orc_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2;

SET hive.optimize.index.filter=true;
SELECT SUM(HASH(t)) FROM orc_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2;
SET hive.optimize.index.filter=false;

EXPLAIN SELECT SUM(HASH(t)) FROM orc_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2;

SET hive.optimize.index.filter=true;
EXPLAIN SELECT SUM(HASH(t)) FROM orc_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2;
SET hive.optimize.index.filter=false;

SELECT t, s FROM orc_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ORDER BY s;

SET hive.optimize.index.filter=true;
SELECT t, s FROM orc_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ORDER BY s;
SET hive.optimize.index.filter=false;

EXPLAIN SELECT t, s FROM orc_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ORDER BY s;

SET hive.optimize.index.filter=true;
EXPLAIN SELECT t, s FROM orc_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ORDER BY s;
SET hive.optimize.index.filter=false;

SELECT t, s FROM orc_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;

set hive.optimize.index.filter=true;
SELECT t, s FROM orc_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;
set hive.optimize.index.filter=false;

EXPLAIN SELECT t, s FROM orc_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;

SET hive.optimize.index.filter=true;
EXPLAIN SELECT t, s FROM orc_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;
SET hive.optimize.index.filter=false;

SELECT t, si, d, s FROM orc_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;

SET hive.optimize.index.filter=true;
SELECT t, si, d, s FROM orc_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;
SET hive.optimize.index.filter=false;

EXPLAIN SELECT t, si, d, s FROM orc_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;

SET hive.optimize.index.filter=true;
EXPLAIN SELECT t, si, d, s FROM orc_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;
SET hive.optimize.index.filter=false;

SELECT t, si, d, s FROM orc_pred
  WHERE t > 10
  AND t <> 101
  AND d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  SORT BY s DESC
  LIMIT 3;

SET hive.optimize.index.filter=true;
SELECT t, si, d, s FROM orc_pred
  WHERE t > 10
  AND t <> 101
  AND d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  SORT BY s DESC
  LIMIT 3;
SET hive.optimize.index.filter=false;

EXPLAIN SELECT t, si, d, s FROM orc_pred
  WHERE t > 10
  AND t <> 101
  AND d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  SORT BY s DESC
  LIMIT 3;

SET hive.optimize.index.filter=true;
EXPLAIN SELECT t, si, d, s FROM orc_pred
  WHERE t > 10
  AND t <> 101
  AND d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  SORT BY s DESC
  LIMIT 3;
SET hive.optimize.index.filter=false;
