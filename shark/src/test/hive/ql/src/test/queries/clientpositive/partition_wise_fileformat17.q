-- HIVE-5199, HIVE-5285 : CustomSerDe(1, 2, 3) are used here.
-- The final results should be all NULL columns deserialized using 
-- CustomSerDe(1, 2, 3) irrespective of the inserted values

DROP TABLE PW17;
ADD JAR ../build/ql/test/test-serdes.jar;
CREATE TABLE PW17(USER STRING, COMPLEXDT ARRAY<INT>) PARTITIONED BY (YEAR STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe1';
LOAD DATA LOCAL INPATH '../data/files/pw17.txt' INTO TABLE PW17 PARTITION (YEAR='1');
ALTER TABLE PW17 PARTITION(YEAR='1') SET SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe2';
ALTER TABLE PW17 SET SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe1';
-- Without the fix HIVE-5199, will throw cast exception via FetchOperator
SELECT * FROM PW17;

-- Test for non-parititioned table. 
DROP TABLE PW17_2;
CREATE TABLE PW17_2(USER STRING, COMPLEXDT ARRAY<INT>) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe1';
LOAD DATA LOCAL INPATH '../data/files/pw17.txt' INTO TABLE PW17_2;
-- Without the fix HIVE-5199, will throw cast exception via MapOperator
SELECT COUNT(*) FROM PW17_2;

DROP TABLE PW17_3;
CREATE TABLE PW17_3(USER STRING, COMPLEXDT ARRAY<ARRAY<INT> >) PARTITIONED BY (YEAR STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe3';
LOAD DATA LOCAL INPATH '../data/files/pw17.txt' INTO TABLE PW17_3 PARTITION (YEAR='1');
ALTER TABLE PW17_3 PARTITION(YEAR='1') SET SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe2';
ALTER TABLE PW17_3 SET SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe3';
-- Without the fix HIVE-5285, will throw cast exception via FetchOperator
SELECT * FROM PW17;

DROP TABLE PW17_4;
CREATE TABLE PW17_4(USER STRING, COMPLEXDT ARRAY<ARRAY<INT> >) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.CustomSerDe3';
LOAD DATA LOCAL INPATH '../data/files/pw17.txt' INTO TABLE PW17_4;
-- Without the fix HIVE-5285, will throw cast exception via MapOperator
SELECT COUNT(*) FROM PW17_4;

