set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- TestSerDe is a user defined serde where the default delimiter is Ctrl-B
-- the user is overwriting it with ctrlC

DROP TABLE INPUT16_CC;
ADD JAR ${system:maven.local.repository}/org/apache/hive/hive-it-test-serde/${system:hive.version}/hive-it-test-serde-${system:hive.version}.jar;
CREATE TABLE INPUT16_CC(KEY STRING, VALUE STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.TestSerDe'  with serdeproperties ('testserde.default.serialization.format'='\003', 'dummy.prop.not.used'='dummyy.val') STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv1_cc.txt' INTO TABLE INPUT16_CC;
SELECT INPUT16_CC.VALUE, INPUT16_CC.KEY FROM INPUT16_CC;

