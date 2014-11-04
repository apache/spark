set hive.fetch.task.conversion=more;

-- 'true' cases:

SELECT CAST(CAST(1 AS TINYINT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(2 AS SMALLINT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(-4 AS INT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(-444 AS BIGINT) AS BOOLEAN) FROM src tablesample (1 rows);

SELECT CAST(CAST(7.0 AS FLOAT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(-8.0 AS DOUBLE) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(-99.0 AS DECIMAL) AS BOOLEAN) FROM src tablesample (1 rows);

SELECT CAST(CAST('Foo' AS STRING) AS BOOLEAN) FROM src tablesample (1 rows);

SELECT CAST(CAST('2011-05-06 07:08:09' as timestamp) AS BOOLEAN) FROM src tablesample (1 rows);

-- 'false' cases: 

SELECT CAST(CAST(0 AS TINYINT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(0 AS SMALLINT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(0 AS INT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(0 AS BIGINT) AS BOOLEAN) FROM src tablesample (1 rows);

SELECT CAST(CAST(0.0 AS FLOAT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(0.0 AS DOUBLE) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(0.0 AS DECIMAL) AS BOOLEAN) FROM src tablesample (1 rows);

SELECT CAST(CAST('' AS STRING) AS BOOLEAN) FROM src tablesample (1 rows);

SELECT CAST(CAST(0 as timestamp) AS BOOLEAN) FROM src tablesample (1 rows);

-- 'NULL' cases:
SELECT CAST(NULL AS BOOLEAN) FROM src tablesample (1 rows);

SELECT CAST(CAST(NULL AS TINYINT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(NULL AS SMALLINT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(NULL AS INT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(NULL AS BIGINT) AS BOOLEAN) FROM src tablesample (1 rows);

SELECT CAST(CAST(NULL AS FLOAT) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(NULL AS DOUBLE) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(NULL AS DECIMAL) AS BOOLEAN) FROM src tablesample (1 rows);

SELECT CAST(CAST(NULL AS STRING) AS BOOLEAN) FROM src tablesample (1 rows);
SELECT CAST(CAST(NULL as timestamp) AS BOOLEAN) FROM src tablesample (1 rows);
