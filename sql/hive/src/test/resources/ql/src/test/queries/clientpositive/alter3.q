create table alter3_src ( col1 string ) stored as textfile ;
load data local inpath '../../data/files/test.dat' overwrite into table alter3_src ;

create table alter3 ( col1 string ) partitioned by (pcol1 string , pcol2 string) stored as sequencefile;

create table alter3_like like alter3;

insert overwrite table alter3 partition (pCol1='test_part:', pcol2='test_part:') select col1 from alter3_src ;
select * from alter3 where pcol1='test_part:' and pcol2='test_part:';


alter table alter3 rename to alter3_renamed;
describe extended alter3_renamed;
describe extended alter3_renamed partition (pCol1='test_part:', pcol2='test_part:');
select * from alter3_renamed where pcol1='test_part:' and pcol2='test_part:';

insert overwrite table alter3_like
partition (pCol1='test_part:', pcol2='test_part:')
select col1 from alter3_src;
alter table alter3_like rename to alter3_like_renamed;

describe extended alter3_like_renamed;

-- Cleanup
DROP TABLE alter3_src;
DROP TABLE alter3_renamed;
DROP TABLE alter3_like_renamed;
SHOW TABLES;

-- With non-default Database

CREATE DATABASE alter3_db;
USE alter3_db;
SHOW TABLES;

CREATE TABLE alter3_src (col1 STRING) STORED AS TEXTFILE ;
LOAD DATA LOCAL INPATH '../../data/files/test.dat' OVERWRITE INTO TABLE alter3_src ;

CREATE TABLE alter3 (col1 STRING) PARTITIONED BY (pcol1 STRING, pcol2 STRING) STORED AS SEQUENCEFILE;

CREATE TABLE alter3_like LIKE alter3;

INSERT OVERWRITE TABLE alter3 PARTITION (pCol1='test_part:', pcol2='test_part:') SELECT col1 FROM alter3_src ;
SELECT * FROM alter3 WHERE pcol1='test_part:' AND pcol2='test_part:';

ALTER TABLE alter3 RENAME TO alter3_renamed;
DESCRIBE EXTENDED alter3_renamed;
DESCRIBE EXTENDED alter3_renamed PARTITION (pCol1='test_part:', pcol2='test_part:');
SELECT * FROM alter3_renamed WHERE pcol1='test_part:' AND pcol2='test_part:';

INSERT OVERWRITE TABLE alter3_like
PARTITION (pCol1='test_part:', pcol2='test_part:')
SELECT col1 FROM alter3_src;
ALTER TABLE alter3_like RENAME TO alter3_like_renamed;

DESCRIBE EXTENDED alter3_like_renamed;
