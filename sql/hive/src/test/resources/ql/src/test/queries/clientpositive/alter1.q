create table alter1(a int, b int);
describe extended alter1;
alter table alter1 set tblproperties ('a'='1', 'c'='3');
describe extended alter1;
alter table alter1 set tblproperties ('a'='1', 'c'='4', 'd'='3');
describe extended alter1;

alter table alter1 set tblproperties ('EXTERNAL'='TRUE');
describe extended alter1;
alter table alter1 set tblproperties ('EXTERNAL'='FALSE');
describe extended alter1;

alter table alter1 set serdeproperties('s1'='9');
describe extended alter1;
alter table alter1 set serdeproperties('s1'='10', 's2' ='20');
describe extended alter1;

add jar ${system:maven.local.repository}/org/apache/hive/hive-it-test-serde/${system:hive.version}/hive-it-test-serde-${system:hive.version}.jar;
alter table alter1 set serde 'org.apache.hadoop.hive.serde2.TestSerDe' with serdeproperties('s1'='9');
describe extended alter1;

alter table alter1 set serde 'org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe';
describe extended alter1;

alter table alter1 replace columns (a int, b int, c string);
describe alter1;

-- Cleanup
DROP TABLE alter1;
SHOW TABLES;

-- With non-default Database

CREATE DATABASE alter1_db;
USE alter1_db;
SHOW TABLES;

CREATE TABLE alter1(a INT, b INT);
DESCRIBE EXTENDED alter1;

ALTER TABLE alter1 SET TBLPROPERTIES ('a'='1', 'c'='3');
DESCRIBE EXTENDED alter1;

ALTER TABLE alter1 SET TBLPROPERTIES ('a'='1', 'c'='4', 'd'='3');
DESCRIBE EXTENDED alter1;

ALTER TABLE alter1 SET TBLPROPERTIES ('EXTERNAL'='TRUE');
DESCRIBE EXTENDED alter1;

ALTER TABLE alter1 SET TBLPROPERTIES ('EXTERNAL'='FALSE');
DESCRIBE EXTENDED alter1;

ALTER TABLE alter1 SET SERDEPROPERTIES('s1'='9');
DESCRIBE EXTENDED alter1;

ALTER TABLE alter1 SET SERDEPROPERTIES('s1'='10', 's2' ='20');
DESCRIBE EXTENDED alter1;

add jar ${system:maven.local.repository}/org/apache/hive/hive-it-test-serde/${system:hive.version}/hive-it-test-serde-${system:hive.version}.jar;
ALTER TABLE alter1 SET SERDE 'org.apache.hadoop.hive.serde2.TestSerDe' WITH SERDEPROPERTIES ('s1'='9');
DESCRIBE EXTENDED alter1;

ALTER TABLE alter1 SET SERDE 'org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe';
DESCRIBE EXTENDED alter1;

ALTER TABLE alter1 REPLACE COLUMNS (a int, b int, c string);
DESCRIBE alter1;

DROP TABLE alter1;
USE default;
DROP DATABASE alter1_db;
