CREATE TABLE avro1
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "org.apache.hive",
  "name": "big_old_schema",
  "type": "record",
  "fields": [
    { "name":"string1", "type":"string" },
    { "name":"int1", "type":"int" },
    { "name":"tinyint1", "type":"int" },
    { "name":"smallint1", "type":"int" },
    { "name":"bigint1", "type":"long" },
    { "name":"boolean1", "type":"boolean" },
    { "name":"float1", "type":"float" },
    { "name":"double1", "type":"double" },
    { "name":"list1", "type":{"type":"array", "items":"string"} },
    { "name":"map1", "type":{"type":"map", "values":"int"} },
    { "name":"struct1", "type":{"type":"record", "name":"struct1_name", "fields": [
          { "name":"sInt", "type":"int" }, { "name":"sBoolean", "type":"boolean" }, { "name":"sString", "type":"string" } ] } },
    { "name":"union1", "type":["float", "boolean", "string"] },
    { "name":"enum1", "type":{"type":"enum", "name":"enum1_values", "symbols":["BLUE","RED", "GREEN"]} },
    { "name":"nullableint", "type":["int", "null"] },
    { "name":"bytes1", "type":"bytes" },
    { "name":"fixed1", "type":{"type":"fixed", "name":"threebytes", "size":3} }
  ] }');

DESCRIBE avro1;

