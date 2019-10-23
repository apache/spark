Thrift commands to generate files from TCLIService.thrift:
--------------------
thrift --gen java:beans,hashcode -o src/gen/thrift if/TCLIService.thrift
thrift --gen cpp -o src/gen/thrift if/TCLIService.thrift
thrift --gen py -o src/gen/thrift if/TCLIService.thrift
thrift --gen rb -o src/gen/thrift if/TCLIService.thrift
