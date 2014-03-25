
create table test1(col1 string) partitioned by (partitionId int);
insert overwrite table test1 partition (partitionId=1)
  select key from src limit 10;

 FROM (
 FROM test1
 SELECT partitionId, 111 as col2, 222 as col3, 333 as col4
 WHERE partitionId = 1
 DISTRIBUTE BY partitionId
 SORT BY partitionId
 ) b

SELECT TRANSFORM(
 b.partitionId,b.col2,b.col3,b.col4
 )

 USING 'cat' as (a,b,c,d);