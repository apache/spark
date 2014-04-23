set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 5 MR jobs will be generated.
-- When Correlation Optimizer is turned on, the subquery tmp will be evalauted
-- in a single MR job (including the subquery b, the subquery d, and b join d).
-- At the reduce side of the MR job evaluating tmp, two operation paths
-- (for subquery b and d) have different depths. The path starting from subquery b
-- is JOIN->GBY->JOIN, which has a depth of 3. While, the path starting from subquery d
-- is JOIN->JOIN. We should be able to handle this case.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT b.key AS key, b.cnt AS cnt, d.value AS value
      FROM (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) b
      JOIN (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) d
      ON b.key = d.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT b.key AS key, b.cnt AS cnt, d.value AS value
      FROM (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) b
      JOIN (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) d
      ON b.key = d.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT b.key AS key, b.cnt AS cnt, d.value AS value
      FROM (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) b
      JOIN (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) d
      ON b.key = d.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT b.key AS key, b.cnt AS cnt, d.value AS value
      FROM (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) b
      JOIN (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) d
      ON b.key = d.key) tmp;

set hive.optimize.correlation=true;
set hive.auto.convert.join=true;
-- Enable hive.auto.convert.join.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT b.key AS key, b.cnt AS cnt, d.value AS value
      FROM (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) b
      JOIN (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) d
      ON b.key = d.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT b.key AS key, b.cnt AS cnt, d.value AS value
      FROM (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) b
      JOIN (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) d
      ON b.key = d.key) tmp;

set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT d.key AS key, d.cnt AS cnt, b.value as value
      FROM (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) b
      JOIN (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) d
      ON b.key = d.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT d.key AS key, d.cnt AS cnt, b.value as value
      FROM (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) b
      JOIN (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) d
      ON b.key = d.key) tmp;

set hive.optimize.correlation=true;
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT d.key AS key, d.cnt AS cnt, b.value as value
      FROM (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) b
      JOIN (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) d
      ON b.key = d.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT d.key AS key, d.cnt AS cnt, b.value as value
      FROM (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) b
      JOIN (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) d
      ON b.key = d.key) tmp;

set hive.optimize.correlation=true;
set hive.auto.convert.join=true;
-- Enable hive.auto.convert.join.
EXPLAIN
SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT d.key AS key, d.cnt AS cnt, b.value as value
      FROM (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) b
      JOIN (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) d
      ON b.key = d.key) tmp;

SELECT SUM(HASH(tmp.key)), SUM(HASH(tmp.cnt)), SUM(HASH(tmp.value))
FROM (SELECT d.key AS key, d.cnt AS cnt, b.value as value
      FROM (SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) b
      JOIN (SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) d
      ON b.key = d.key) tmp;
