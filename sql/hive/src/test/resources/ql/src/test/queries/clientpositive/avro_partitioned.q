-- verify that new joins bring in correct schemas (including evolved schemas)
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

LOAD DATA LOCAL INPATH '../data/files/episodes.avro' INTO TABLE episodes;

CREATE TABLE episodes_partitioned
PARTITIONED BY (doctor_pt INT)
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

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE episodes_partitioned PARTITION (doctor_pt) SELECT title, air_date, doctor, doctor as doctor_pt FROM episodes;

SELECT * FROM episodes_partitioned WHERE doctor_pt > 6 ORDER BY air_date;

-- Verify that Fetch works in addition to Map
SELECT * FROM episodes_partitioned LIMIT 5;
-- Fetch w/filter to specific partition
SELECT * FROM episodes_partitioned WHERE doctor_pt = 6;
-- Fetch w/non-existant partition
SELECT * FROM episodes_partitioned WHERE doctor_pt = 7 LIMIT 5;
