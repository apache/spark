-- TestSerDe is a user defined serde where the default delimiter is Ctrl-B
DROP TABLE INPUT16;
ADD JAR ../data/files/TestSerDe.jar;
CREATE TABLE INPUT16(KEY STRING, VALUE STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.TestSerDe' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/kv1_cb.txt' INTO TABLE INPUT16;
SELECT INPUT16.VALUE, INPUT16.KEY FROM INPUT16;
