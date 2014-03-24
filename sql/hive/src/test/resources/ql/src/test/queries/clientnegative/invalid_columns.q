ADD JAR ../data/files/TestSerDe.jar;
CREATE TABLE DELETEJAR(KEY STRING, VALUE STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.TestSerDe' 
STORED AS TEXTFILE
TBLPROPERTIES('columns'='valid_colname,invalid.colname');
