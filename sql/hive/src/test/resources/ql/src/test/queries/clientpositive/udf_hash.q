set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION hash;
DESCRIBE FUNCTION EXTENDED hash;

EXPLAIN
SELECT hash(CAST(1 AS TINYINT)), hash(CAST(2 AS SMALLINT)),
       hash(3), hash(CAST('123456789012' AS BIGINT)),
       hash(CAST(1.25 AS FLOAT)), hash(CAST(16.0 AS DOUBLE)),
       hash('400'), hash('abc'), hash(TRUE), hash(FALSE),
       hash(1, 2, 3)
FROM src tablesample (1 rows);

SELECT hash(CAST(1 AS TINYINT)), hash(CAST(2 AS SMALLINT)),
       hash(3), hash(CAST('123456789012' AS BIGINT)),
       hash(CAST(1.25 AS FLOAT)), hash(CAST(16.0 AS DOUBLE)),
       hash('400'), hash('abc'), hash(TRUE), hash(FALSE),
       hash(1, 2, 3)
FROM src tablesample (1 rows);
