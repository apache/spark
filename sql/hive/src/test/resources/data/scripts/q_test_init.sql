--
-- Table src
--
DROP TABLE IF EXISTS src;

CREATE TABLE src (key STRING, value STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt" INTO TABLE src;

--
-- Table src1
--
DROP TABLE IF EXISTS src1;

CREATE TABLE src1 (key STRING, value STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv3.txt" INTO TABLE src1;

--
-- Table src_json
--
DROP TABLE IF EXISTS src_json;

CREATE TABLE src_json (json STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/json.txt" INTO TABLE src_json;


--
-- Table src_sequencefile
--
DROP TABLE IF EXISTS src_sequencefile;

CREATE TABLE src_sequencefile (key STRING, value STRING) STORED AS SEQUENCEFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.seq" INTO TABLE src_sequencefile;


--
-- Table src_thrift
--
DROP TABLE IF EXISTS src_thrift;

CREATE TABLE src_thrift
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
WITH SERDEPROPERTIES (
  'serialization.class' = 'org.apache.hadoop.hive.serde2.thrift.test.Complex',
  'serialization.format' = 'com.facebook.thrift.protocol.TBinaryProtocol')
STORED AS SEQUENCEFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/complex.seq" INTO TABLE src_thrift;


--
-- Table srcbucket
--
DROP TABLE IF EXISTS srcbucket;

CREATE TABLE srcbucket (key INT, value STRING)
CLUSTERED BY (key) INTO 2 BUCKETS
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/srcbucket0.txt" INTO TABLE srcbucket;
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/srcbucket1.txt" INTO TABLE srcbucket;


--
-- Table srcbucket2
--
DROP TABLE IF EXISTS srcbucket2;

CREATE TABLE srcbucket2 (key INT, value STRING)
CLUSTERED BY (key) INTO 4 BUCKETS
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/srcbucket20.txt" INTO TABLE srcbucket2;
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/srcbucket21.txt" INTO TABLE srcbucket2;


--
-- Table srcpart
--
DROP TABLE IF EXISTS srcpart;

CREATE TABLE srcpart (key STRING, value STRING)
PARTITIONED BY (ds STRING, hr STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-08", hr="11");

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-08", hr="12");

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-09", hr="11");

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-09", hr="12");


DROP TABLE IF EXISTS primitives;
CREATE TABLE primitives (
  id INT,
  bool_col BOOLEAN,
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  date_string_col STRING,
  string_col STRING,
  timestamp_col TIMESTAMP)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY '\\'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090101.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=1);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090201.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=2);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090301.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=3);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090401.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=4);

