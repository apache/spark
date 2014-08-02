-- Ensure Enum fields are converted to strings (instead of struct<value:int>)

create table convert_enum_to_string
  partitioned by (b string)
  row format serde "org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer"
    with serdeproperties (
      "serialization.class"="org.apache.hadoop.hive.serde2.thrift.test.MegaStruct",
      "serialization.format"="org.apache.thrift.protocol.TBinaryProtocol");

describe convert_enum_to_string;
