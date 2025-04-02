set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION nvl;
DESCRIBE FUNCTION EXTENDED nvl;

EXPLAIN
SELECT NVL( 1 , 2 ) AS COL1,
       NVL( NULL, 5 ) AS COL2
FROM src tablesample (1 rows);

SELECT NVL( 1 , 2 ) AS COL1,
       NVL( NULL, 5 ) AS COL2
FROM src tablesample (1 rows);

