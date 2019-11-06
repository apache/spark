Thrift commands to generate files from TCLIService.thrift :
--------------------
Please use Thrift 0.9.3 available from https://www.apache.org/dist/thrift/0.9.3

`thrift --gen java:beans,hashcode,generated_annotations=undated -o src/gen/thrift if/TCLIService.thrift`
