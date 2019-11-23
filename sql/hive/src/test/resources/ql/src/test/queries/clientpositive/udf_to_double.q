set hive.fetch.task.conversion=more;

-- Conversion of main primitive types to Double type:
SELECT CAST(NULL AS DOUBLE) FROM src tablesample (1 rows);

SELECT CAST(TRUE AS DOUBLE) FROM src tablesample (1 rows);

SELECT CAST(CAST(-7 AS TINYINT) AS DOUBLE) FROM src tablesample (1 rows);
SELECT CAST(CAST(-18 AS SMALLINT) AS DOUBLE) FROM src tablesample (1 rows);
SELECT CAST(-129 AS DOUBLE) FROM src tablesample (1 rows);
SELECT CAST(CAST(-1025 AS BIGINT) AS DOUBLE) FROM src tablesample (1 rows);

SELECT CAST(CAST(-3.14 AS FLOAT) AS DOUBLE) FROM src tablesample (1 rows);
SELECT CAST(CAST(-3.14 AS DECIMAL(3,2)) AS DOUBLE) FROM src tablesample (1 rows);

SELECT CAST('-38.14' AS DOUBLE) FROM src tablesample (1 rows);

