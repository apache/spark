FROM src_thrift
SELECT size(src_thrift.lint), size(src_thrift.lintstring), size(src_thrift.mstringstring) where src_thrift.lint IS NOT NULL AND NOT (src_thrift.mstringstring IS NULL)
