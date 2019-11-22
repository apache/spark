CREATE TABLE T1(key INT);
LOAD DATA LOCAL INPATH '../../data/files/leftsemijoin_mr_t1.txt' INTO TABLE T1;
CREATE TABLE T2(key INT);
LOAD DATA LOCAL INPATH '../../data/files/leftsemijoin_mr_t2.txt' INTO TABLE T2;

-- Run this query using TestMinimrCliDriver

SELECT * FROM T1;
SELECT * FROM T2;

set hive.auto.convert.join=false;
set mapreduce.job.reduces=2;

set hive.join.emit.interval=100;

SELECT T1.key FROM T1 LEFT SEMI JOIN (SELECT key FROM T2 SORT BY key) tmp ON (T1.key=tmp.key);

set hive.join.emit.interval=1;

SELECT T1.key FROM T1 LEFT SEMI JOIN (SELECT key FROM T2 SORT BY key) tmp ON (T1.key=tmp.key);
