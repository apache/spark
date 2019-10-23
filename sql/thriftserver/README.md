Thrift commands to generate files from TCLIService.thrift:
--------------------
thrift --gen java:beans,hashcode -o src/gen/thrift if/TCLIService.thrift
