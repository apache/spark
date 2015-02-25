set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION isnull;
DESCRIBE FUNCTION EXTENDED isnull;

DESCRIBE FUNCTION isnotnull;
DESCRIBE FUNCTION EXTENDED isnotnull;


EXPLAIN
SELECT NULL IS NULL,
       1 IS NOT NULL, 
       'my string' IS NOT NULL
FROM src
WHERE true IS NOT NULL LIMIT 1;


SELECT NULL IS NULL,
       1 IS NOT NULL, 
       'my string' IS NOT NULL
FROM src
WHERE true IS NOT NULL LIMIT 1;


EXPLAIN
FROM src_thrift
SELECT src_thrift.lint IS NOT NULL, 
       src_thrift.lintstring IS NOT NULL, 
       src_thrift.mstringstring IS NOT NULL
WHERE  src_thrift.lint IS NOT NULL 
       AND NOT (src_thrift.mstringstring IS NULL) LIMIT 1;


FROM src_thrift
SELECT src_thrift.lint IS NOT NULL, 
       src_thrift.lintstring IS NOT NULL, 
       src_thrift.mstringstring IS NOT NULL
WHERE  src_thrift.lint IS NOT NULL 
       AND NOT (src_thrift.mstringstring IS NULL) LIMIT 1;
