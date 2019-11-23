-- verify that we can update the table properties
CREATE TABLE avro2
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{ "namespace": "org.apache.hive",
  "name": "first_schema",
  "type": "record",
  "fields": [
    { "name":"string1", "type":"string" },
    { "name":"string2", "type":"string" }
  ] }');

DESCRIBE avro2;

ALTER TABLE avro2 SET TBLPROPERTIES ('avro.schema.literal'='{ "namespace": "org.apache.hive",
  "name": "second_schema",
  "type": "record",
  "fields": [
    { "name":"int1", "type":"int" },
    { "name":"float1", "type":"float" },
  { "name":"double1", "type":"double" }
  ] }');

DESCRIBE avro2;

