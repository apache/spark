create table s1 as select * from src where key = 0;

set hive.auto.convert.join.noconditionaltask=false;
EXPLAIN
SELECT * FROM s1 src1 LEFT OUTER JOIN s1 src2 ON (src1.key = src2.key AND src2.key > 10);
SELECT * FROM s1 src1 LEFT OUTER JOIN s1 src2 ON (src1.key = src2.key AND src2.key > 10);

set hive.auto.convert.join.noconditionaltask=true;

-- Make sure the big table is chosen correctly as part of HIVE-4146
EXPLAIN
SELECT * FROM s1 src1 LEFT OUTER JOIN s1 src2 ON (src1.key = src2.key AND src2.key > 10);
SELECT * FROM s1 src1 LEFT OUTER JOIN s1 src2 ON (src1.key = src2.key AND src2.key > 10);



