set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=200;

-- EXCLUDE_OS_WINDOWS
-- excluded on windows because of difference in file name encoding logic

DROP TABLE escape1;
DROP TABLE escape_raw;

CREATE TABLE escape_raw (s STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/escapetest.txt' INTO TABLE  escape_raw;

SELECT count(*) from escape_raw;
SELECT * from escape_raw;

CREATE TABLE escape1 (a STRING) PARTITIONED BY (ds STRING, part STRING);
INSERT OVERWRITE TABLE escape1 PARTITION (ds='1', part) SELECT '1', s from 
escape_raw;

SELECT count(*) from escape1;
SELECT * from escape1;
SHOW PARTITIONS escape1;

ALTER TABLE escape1 DROP PARTITION (ds='1');
SHOW PARTITIONS escape1;

DROP TABLE escape1;
DROP TABLE escape_raw;
