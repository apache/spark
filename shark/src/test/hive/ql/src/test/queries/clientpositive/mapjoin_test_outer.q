set hive.auto.convert.join = false;
--HIVE-2101 mapjoin sometimes gives wrong results if there is a filter in the on condition

create table dest_1 (key STRING, value STRING) stored as textfile;
insert overwrite table dest_1 select * from src1 order by src1.value limit 8;
insert into table dest_1 select "333444","555666" from src1 limit 1;

create table dest_2 (key STRING, value STRING) stored as textfile;

insert into table dest_2 select * from dest_1;

SELECT * FROM src1
  RIGHT OUTER JOIN dest_1 src2 ON (src1.key = src2.key)
  JOIN dest_2 src3 ON (src2.key = src3.key)
  SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

explain
SELECT /*+ mapjoin(src1, src2) */ * FROM src1
  RIGHT OUTER JOIN dest_1 src2 ON (src1.key = src2.key)
  JOIN dest_2 src3 ON (src2.key = src3.key)
  SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

SELECT /*+ mapjoin(src1, src2) */ * FROM src1
  RIGHT OUTER JOIN dest_1 src2 ON (src1.key = src2.key)
  JOIN dest_2 src3 ON (src2.key = src3.key)
  SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

SELECT /*+ mapjoin(src1, src2) */ * FROM src1
  RIGHT OUTER JOIN dest_1 src2 ON (src1.key = src2.key)
  JOIN dest_2 src3 ON (src1.key = src3.key)
  SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

set hive.auto.convert.join = true;

SELECT * FROM src1
  LEFT OUTER JOIN dest_1 src2 ON (src1.key = src2.key)
  JOIN dest_2 src3 ON (src1.key = src3.key)
  SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

SELECT * FROM src1
  LEFT OUTER JOIN dest_1 src2 ON (src1.key = src2.key)
  JOIN dest_2 src3 ON (src2.key = src3.key)
  SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

explain
SELECT * FROM src1
  RIGHT OUTER JOIN dest_1 src2 ON (src1.key = src2.key)
  JOIN dest_2 src3 ON (src2.key = src3.key)
  SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

SELECT * FROM src1
  RIGHT OUTER JOIN dest_1 src2 ON (src1.key = src2.key)
  JOIN dest_2 src3 ON (src2.key = src3.key)
  SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;
