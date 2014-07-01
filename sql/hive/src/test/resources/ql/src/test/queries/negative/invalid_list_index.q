FROM src_thrift
INSERT OVERWRITE TABLE dest1 SELECT src_thrift.lint[0], src_thrift.lstring['abc']
