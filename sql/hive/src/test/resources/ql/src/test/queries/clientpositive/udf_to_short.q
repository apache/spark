set hive.fetch.task.conversion=more;

-- Conversion of main primitive types to Short type:
SELECT CAST(NULL AS SMALLINT) FROM src tablesample (1 rows);

SELECT CAST(TRUE AS SMALLINT) FROM src tablesample (1 rows);

SELECT CAST(CAST(-18 AS TINYINT) AS SMALLINT) FROM src tablesample (1 rows);
SELECT CAST(-129 AS SMALLINT) FROM src tablesample (1 rows);
SELECT CAST(CAST(-1025 AS BIGINT) AS SMALLINT) FROM src tablesample (1 rows);

SELECT CAST(CAST(-3.14 AS DOUBLE) AS SMALLINT) FROM src tablesample (1 rows);
SELECT CAST(CAST(-3.14 AS FLOAT) AS SMALLINT) FROM src tablesample (1 rows);
SELECT CAST(CAST(-3.14 AS DECIMAL) AS SMALLINT) FROM src tablesample (1 rows);

SELECT CAST('-38' AS SMALLINT) FROM src tablesample (1 rows);

