CREATE TABLE t1(col1 INT, col2 STRING);

DECLARE all = 1;
DECLARE a = 1;

-- Precedence with group by ALL
SELECT col1, SUM(col2) FROM t1 GROUP BY ALL;
SELECT col1, SUM(col2) FROM t1 GROUP BY ALL ORDER BY ALL;
SELECT col1, col2 FROM t1 ORDER BY ALL;

-- Precedence with group by alias
SELECT col1 AS a, SUM(col2) FROM t1 GROUP BY a;

DROP TEMP VARIABLE a;
DROP TEMP VARIABLE all;

DROP TABLE t1;
