ADD JAR ${system:maven.local.repository}/org/apache/hive/hive-it-test-serde/${system:hive.version}/hive-it-test-serde-${system:hive.version}.jar;

CREATE TABLE xyz(KEY STRING, VALUE STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.TestSerDe' 
STORED AS TEXTFILE
TBLPROPERTIES('columns'='valid_colname,invalid.colname')
;

describe xyz;