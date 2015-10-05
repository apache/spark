set hive.fetch.task.conversion=more;

-- Conversion of main primitive types to String type:
SELECT CAST(NULL AS STRING) FROM src tablesample (1 rows);

SELECT CAST(TRUE AS STRING) FROM src tablesample (1 rows);

SELECT CAST(CAST(1 AS TINYINT) AS STRING) FROM src tablesample (1 rows);
SELECT CAST(CAST(-18 AS SMALLINT) AS STRING) FROM src tablesample (1 rows);
SELECT CAST(-129 AS STRING) FROM src tablesample (1 rows);
SELECT CAST(CAST(-1025 AS BIGINT) AS STRING) FROM src tablesample (1 rows);

SELECT CAST(CAST(-3.14 AS DOUBLE) AS STRING) FROM src tablesample (1 rows);
SELECT CAST(CAST(-3.14 AS FLOAT) AS STRING) FROM src tablesample (1 rows);
SELECT CAST(CAST(-3.14 AS DECIMAL(3,2)) AS STRING) FROM src tablesample (1 rows);

SELECT CAST('Foo' AS STRING) FROM src tablesample (1 rows);

