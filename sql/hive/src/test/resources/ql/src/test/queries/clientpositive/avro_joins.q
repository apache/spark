-- verify that new joins bring in correct schemas (including evolved schemas)

CREATE TABLE doctors4
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

DESCRIBE doctors4;

LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' INTO TABLE doctors4;

CREATE TABLE episodes
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "episodes",
  "type": "record",
  "fields": [
    {
      "name":"title",
      "type":"string",
      "doc":"episode title"
    },
    {
      "name":"air_date",
      "type":"string",
      "doc":"initial date"
    },
    {
      "name":"doctor",
      "type":"int",
      "doc":"main actor playing the Doctor in episode"
    }
  ]
}');

DESCRIBE episodes;

LOAD DATA LOCAL INPATH '../../data/files/episodes.avro' INTO TABLE episodes;

SELECT e.title, e.air_date, d.first_name, d.last_name, d.extra_field, e.air_date
FROM doctors4 d JOIN episodes e ON (d.number=e.doctor)
ORDER BY d.last_name, e.title;


