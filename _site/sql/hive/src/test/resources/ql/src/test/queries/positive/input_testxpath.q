FROM src_thrift
SELECT src_thrift.lint[1], src_thrift.lintstring[0].mystring, src_thrift.mstringstring['key_2']
