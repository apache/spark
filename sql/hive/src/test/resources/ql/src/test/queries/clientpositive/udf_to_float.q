set hive.fetch.task.conversion=more;

-- Conversion of main primitive types to Float type:
SELECT CAST(NULL AS FLOAT) FROM src tablesample (1 rows);

SELECT CAST(TRUE AS FLOAT) FROM src tablesample (1 rows);

SELECT CAST(CAST(-7 AS TINYINT) AS FLOAT) FROM src tablesample (1 rows);
SELECT CAST(CAST(-18 AS SMALLINT) AS FLOAT) FROM src tablesample (1 rows);
SELECT CAST(-129 AS FLOAT) FROM src tablesample (1 rows);
SELECT CAST(CAST(-1025 AS BIGINT) AS FLOAT) FROM src tablesample (1 rows);

SELECT CAST(CAST(-3.14 AS DOUBLE) AS FLOAT) FROM src tablesample (1 rows);
SELECT CAST(CAST(-3.14 AS DECIMAL(3,2)) AS FLOAT) FROM src tablesample (1 rows);

SELECT CAST('-38.14' AS FLOAT) FROM src tablesample (1 rows);

