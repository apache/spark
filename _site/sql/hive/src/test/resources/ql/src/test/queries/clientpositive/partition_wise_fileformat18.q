-- HIVE-5202 : Tests for SettableUnionObjectInspectors
-- CustomSerDe(4,5) are used here. 
-- The final results should be all NULL columns deserialized using 
-- CustomSerDe(4, 5) irrespective of the inserted values

DROP TABLE PW18;
ADD JAR ${system:maven.local.repository}/org/apache/hive/hive-it-custom-serde/${system:hive.version}/hive-it-custom-serde-${system:hive.version}.jar;
CREATE TABLE PW18(USER STRING, COMPLEXDT UNIONTYPE<INT, DOUBLE>) PARTITIONED BY (YEAR STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe5';
LOAD DATA LOCAL INPATH '../../data/files/pw17.txt' INTO TABLE PW18 PARTITION (YEAR='1');
ALTER TABLE PW18 PARTITION(YEAR='1') SET SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe4';
-- Without the fix HIVE-5202, will throw unsupported data type exception.
SELECT * FROM PW18;

-- Test for non-parititioned table. 
DROP TABLE PW18_2;
CREATE TABLE PW18_2(USER STRING, COMPLEXDT UNIONTYPE<INT, DOUBLE>) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe5';
LOAD DATA LOCAL INPATH '../../data/files/pw17.txt' INTO TABLE PW18_2;
-- Without the fix HIVE-5202, will throw unsupported data type exception
SELECT COUNT(*) FROM PW18_2;
