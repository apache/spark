FROM SRC_THRIFT
INSERT OVERWRITE TABLE dest1 SELECT src_Thrift.LINT[1], src_thrift.lintstring[0].MYSTRING where src_thrift.liNT[0] > 0
