FROM src_thrift
SELECT field(1, src_thrift.lintstring)
WHERE src_thrift.lintstring IS NOT NULL;
