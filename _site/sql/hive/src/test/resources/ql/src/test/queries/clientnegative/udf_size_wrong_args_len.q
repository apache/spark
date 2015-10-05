FROM src_thrift
SELECT size(src_thrift.lint, src_thrift.lintstring), 
       size()
WHERE  src_thrift.lint IS NOT NULL 
       AND NOT (src_thrift.mstringstring IS NULL) LIMIT 1;
