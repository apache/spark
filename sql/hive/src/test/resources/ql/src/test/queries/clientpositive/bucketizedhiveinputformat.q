set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set mapreduce.input.fileinputformat.split.minsize = 64;

CREATE TABLE T1(name STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE T1;

CREATE TABLE T2(name STRING) STORED AS SEQUENCEFILE;

EXPLAIN INSERT OVERWRITE TABLE T2 SELECT * FROM (
SELECT tmp1.name as name FROM (
  SELECT name, 'MMM' AS n FROM T1) tmp1 
  JOIN (SELECT 'MMM' AS n FROM T1) tmp2
  JOIN (SELECT 'MMM' AS n FROM T1) tmp3
  ON tmp1.n = tmp2.n AND tmp1.n = tmp3.n) ttt LIMIT 5000000;


INSERT OVERWRITE TABLE T2 SELECT * FROM (
SELECT tmp1.name as name FROM (
  SELECT name, 'MMM' AS n FROM T1) tmp1 
  JOIN (SELECT 'MMM' AS n FROM T1) tmp2
  JOIN (SELECT 'MMM' AS n FROM T1) tmp3
  ON tmp1.n = tmp2.n AND tmp1.n = tmp3.n) ttt LIMIT 5000000;

EXPLAIN SELECT COUNT(1) FROM T2;
SELECT COUNT(1) FROM T2;

CREATE TABLE T3(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE T3;
LOAD DATA LOCAL INPATH '../../data/files/kv2.txt' INTO TABLE T3;

EXPLAIN SELECT COUNT(1) FROM T3;
SELECT COUNT(1) FROM T3;
