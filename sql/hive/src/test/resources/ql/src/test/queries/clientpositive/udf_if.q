set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION if;
DESCRIBE FUNCTION EXTENDED if;

EXPLAIN
SELECT IF(TRUE, 1, 2) AS COL1,
       IF(FALSE, CAST(NULL AS STRING), CAST(1 AS STRING)) AS COL2,
       IF(1=1, IF(2=2, 1, 2), IF(3=3, 3, 4)) AS COL3,
       IF(2=2, 1, NULL) AS COL4,
       IF(2=2, NULL, 1) AS COL5,
       IF(IF(TRUE, NULL, FALSE), 1, 2) AS COL6
FROM src tablesample (1 rows);


SELECT IF(TRUE, 1, 2) AS COL1,
       IF(FALSE, CAST(NULL AS STRING), CAST(1 AS STRING)) AS COL2,
       IF(1=1, IF(2=2, 1, 2), IF(3=3, 3, 4)) AS COL3,
       IF(2=2, 1, NULL) AS COL4,
       IF(2=2, NULL, 1) AS COL5,
       IF(IF(TRUE, NULL, FALSE), 1, 2) AS COL6
FROM src tablesample (1 rows);

-- Type conversions
EXPLAIN
SELECT IF(TRUE, CAST(128 AS SMALLINT), CAST(1 AS TINYINT)) AS COL1,
       IF(FALSE, 1, 1.1) AS COL2,
       IF(FALSE, 1, 'ABC') AS COL3,
       IF(FALSE, 'ABC', 12.3) AS COL4
FROM src tablesample (1 rows);

SELECT IF(TRUE, CAST(128 AS SMALLINT), CAST(1 AS TINYINT)) AS COL1,
       IF(FALSE, 1, 1.1) AS COL2,
       IF(FALSE, 1, 'ABC') AS COL3,
       IF(FALSE, 'ABC', 12.3) AS COL4
FROM src tablesample (1 rows);
