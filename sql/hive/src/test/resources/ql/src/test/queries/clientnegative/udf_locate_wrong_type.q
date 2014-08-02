FROM src_thrift
SELECT locate('abcd', src_thrift.lintstring)
WHERE src_thrift.lintstring IS NOT NULL;
