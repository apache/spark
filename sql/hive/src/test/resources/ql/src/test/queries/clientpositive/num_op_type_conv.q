set hive.fetch.task.conversion=more;

EXPLAIN SELECT null + 7, 1.0 - null, null + null,
               CAST(21 AS BIGINT) % CAST(5 AS TINYINT),
               CAST(21 AS BIGINT) % CAST(21 AS BIGINT),
               9 % "3" FROM src LIMIT 1;

SELECT null + 7, 1.0 - null, null + null,
       CAST(21 AS BIGINT) % CAST(5 AS TINYINT),
       CAST(21 AS BIGINT) % CAST(21 AS BIGINT),
       9 % "3" FROM src LIMIT 1;
