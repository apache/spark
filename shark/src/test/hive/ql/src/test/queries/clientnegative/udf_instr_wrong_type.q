FROM src_thrift
SELECT instr('abcd', src_thrift.lintstring)
WHERE src_thrift.lintstring IS NOT NULL;
