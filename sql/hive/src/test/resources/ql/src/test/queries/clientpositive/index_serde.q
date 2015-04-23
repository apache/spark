set hive.stats.dbclass=fs;
-- Want to ensure we can build and use indices on tables stored with SerDes
-- Build the (Avro backed) table
CREATE TABLE doctors 
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
    }
  ]
}');

DESCRIBE doctors;

LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' INTO TABLE doctors;

-- Create and build an index
CREATE INDEX doctors_index ON TABLE doctors(number) AS 'COMPACT' WITH DEFERRED REBUILD;
DESCRIBE EXTENDED default__doctors_doctors_index__;
ALTER INDEX doctors_index ON doctors REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

EXPLAIN SELECT * FROM doctors WHERE number > 6 ORDER BY number;
SELECT * FROM doctors WHERE number > 6 ORDER BY number;

DROP INDEX doctors_index ON doctors;
DROP TABLE doctors;
