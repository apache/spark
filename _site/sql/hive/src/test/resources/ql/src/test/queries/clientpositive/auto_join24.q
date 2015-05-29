set hive.auto.convert.join = true;

create table tst1(key STRING, cnt INT);

INSERT OVERWRITE TABLE tst1
SELECT a.key, count(1) FROM src a group by a.key;

explain 
SELECT sum(a.cnt)  FROM tst1 a JOIN tst1 b ON a.key = b.key;

SELECT sum(a.cnt)  FROM tst1 a JOIN tst1 b ON a.key = b.key;


