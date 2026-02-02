set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;

CREATE TABLE tbl1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

insert overwrite table tbl1 select * from src where key < 10;
insert overwrite table tbl2 select * from src where key < 10;

CREATE TABLE dest1(k1 int, k2 int);
CREATE TABLE dest2(k1 string, k2 string);

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.auto.convert.sortmerge.join.to.mapjoin=false;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.join=true;

-- A SMB join followed by a multi-insert
explain 
from (
  SELECT a.key key1, a.value value1, b.key key2, b.value value2 
  FROM tbl1 a JOIN tbl2 b 
  ON a.key = b.key ) subq
INSERT OVERWRITE TABLE dest1 select key1, key2
INSERT OVERWRITE TABLE dest2 select value1, value2;

from (
  SELECT a.key key1, a.value value1, b.key key2, b.value value2 
  FROM tbl1 a JOIN tbl2 b 
  ON a.key = b.key ) subq
INSERT OVERWRITE TABLE dest1 select key1, key2
INSERT OVERWRITE TABLE dest2 select value1, value2;

select * from dest1 order by k1, k2;
select * from dest2 order by k1, k2;

set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=200;

-- A SMB join followed by a multi-insert
explain 
from (
  SELECT a.key key1, a.value value1, b.key key2, b.value value2 
  FROM tbl1 a JOIN tbl2 b 
  ON a.key = b.key ) subq
INSERT OVERWRITE TABLE dest1 select key1, key2
INSERT OVERWRITE TABLE dest2 select value1, value2;

from (
  SELECT a.key key1, a.value value1, b.key key2, b.value value2 
  FROM tbl1 a JOIN tbl2 b 
  ON a.key = b.key ) subq
INSERT OVERWRITE TABLE dest1 select key1, key2
INSERT OVERWRITE TABLE dest2 select value1, value2;

select * from dest1 order by k1, k2;
select * from dest2 order by k1, k2;

set hive.auto.convert.sortmerge.join.to.mapjoin=true;
-- A SMB join followed by a multi-insert
explain 
from (
  SELECT a.key key1, a.value value1, b.key key2, b.value value2 
  FROM tbl1 a JOIN tbl2 b 
  ON a.key = b.key ) subq
INSERT OVERWRITE TABLE dest1 select key1, key2
INSERT OVERWRITE TABLE dest2 select value1, value2;

from (
  SELECT a.key key1, a.value value1, b.key key2, b.value value2 
  FROM tbl1 a JOIN tbl2 b 
  ON a.key = b.key ) subq
INSERT OVERWRITE TABLE dest1 select key1, key2
INSERT OVERWRITE TABLE dest2 select value1, value2;

select * from dest1 order by k1, k2;
select * from dest2 order by k1, k2;
