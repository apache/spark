set hive.fetch.task.conversion=more;

-- Conversion of main primitive types to Long type:
SELECT CAST(NULL AS BIGINT) FROM src tablesample (1 rows);

SELECT CAST(TRUE AS BIGINT) FROM src tablesample (1 rows);

SELECT CAST(CAST(-7 AS TINYINT) AS BIGINT) FROM src tablesample (1 rows);
SELECT CAST(CAST(-18 AS SMALLINT) AS BIGINT) FROM src tablesample (1 rows);
SELECT CAST(-129 AS BIGINT) FROM src tablesample (1 rows);

SELECT CAST(CAST(-3.14 AS DOUBLE) AS BIGINT) FROM src tablesample (1 rows);
SELECT CAST(CAST(-3.14 AS FLOAT) AS BIGINT) FROM src tablesample (1 rows);
SELECT CAST(CAST(-3.14 AS DECIMAL) AS BIGINT) FROM src tablesample (1 rows);

SELECT CAST('-38' AS BIGINT) FROM src tablesample (1 rows);

