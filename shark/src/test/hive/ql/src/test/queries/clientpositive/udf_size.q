DESCRIBE FUNCTION size;
DESCRIBE FUNCTION EXTENDED size;

EXPLAIN
FROM src_thrift
SELECT size(src_thrift.lint), 
       size(src_thrift.lintstring), 
       size(src_thrift.mstringstring),
       size(null)
WHERE  src_thrift.lint IS NOT NULL 
       AND NOT (src_thrift.mstringstring IS NULL) LIMIT 1;


FROM src_thrift
SELECT size(src_thrift.lint), 
       size(src_thrift.lintstring), 
       size(src_thrift.mstringstring),
       size(null)
WHERE  src_thrift.lint IS NOT NULL 
       AND NOT (src_thrift.mstringstring IS NULL) LIMIT 1;
