create table int_string
  partitioned by (b string)
  row format serde "org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer"
    with serdeproperties (
      "serialization.class"="org.apache.hadoop.hive.serde2.thrift.test.IntString",
      "serialization.format"="org.apache.thrift.protocol.TBinaryProtocol");
describe extended int_string;
alter table int_string add partition (b='part1');
describe extended int_string partition (b='part1');
