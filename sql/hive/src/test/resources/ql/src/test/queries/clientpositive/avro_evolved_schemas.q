-- verify that new fields in schema get propagated to table scans
CREATE TABLE doctors_with_new_field 
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "doctors",
  "type": "record",
  "fields": [
    {
      "name":"number",
      "type":"int",
      "doc":"Order of playing the role"
    },
    {
      "name":"first_name",
      "type":"string",
      "doc":"first name of actor playing role"
    },
    {
      "name":"last_name",
      "type":"string",
      "doc":"last name of actor playing role"
    },
    {
      "name":"extra_field",
      "type":"string",
      "doc:":"an extra field not in the original file",
      "default":"fishfingers and custard"
    }
  ]
}');

DESCRIBE doctors_with_new_field;

LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' INTO TABLE doctors_with_new_field;

SELECT * FROM doctors_with_new_field ORDER BY first_name;

