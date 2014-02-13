set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;

CREATE TABLE tbl1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl3(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl4(key int, value string) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;

insert overwrite table tbl1 select * from src;
insert overwrite table tbl2 select * from src;
insert overwrite table tbl3 select * from src;
insert overwrite table tbl4 select * from src;

set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=200;
set hive.auto.convert.sortmerge.join.to.mapjoin=false;

-- A SMB join is being followed by a regular join on a non-bucketed table on a different key

-- Three tests below are all the same query with different alias, which changes dispatch order of GenMapRedWalker
-- This is dependent to iteration order of HashMap, so can be meaningless in non-sun jdk
-- b = TS[0]-OP[13]-MAPJOIN[11]-RS[6]-JOIN[8]-SEL[9]-FS[10]
-- c = TS[1]-RS[7]-JOIN[8]
-- a = TS[2]-MAPJOIN[11]
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.value = a.value;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.value = a.value;

-- d = TS[0]-RS[7]-JOIN[8]-SEL[9]-FS[10]
-- b = TS[1]-OP[13]-MAPJOIN[11]-RS[6]-JOIN[8]
-- a = TS[2]-MAPJOIN[11]
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src d on d.value = a.value;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src d on d.value = a.value;

-- b = TS[0]-OP[13]-MAPJOIN[11]-RS[6]-JOIN[8]-SEL[9]-FS[10]
-- a = TS[1]-MAPJOIN[11]
-- h = TS[2]-RS[7]-JOIN[8]
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src h on h.value = a.value;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src h on h.value = a.value;

-- A SMB join is being followed by a regular join on a non-bucketed table on the same key
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.key = a.key;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.key = a.key;

-- A SMB join is being followed by a regular join on a bucketed table on the same key
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl3 c on c.key = a.key;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl3 c on c.key = a.key;

-- A SMB join is being followed by a regular join on a bucketed table on a different key
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl4 c on c.value = a.value;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl4 c on c.value = a.value;

set hive.auto.convert.sortmerge.join.to.mapjoin=true;

-- A SMB join is being followed by a regular join on a non-bucketed table on a different key
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.value = a.value;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.value = a.value;

-- A SMB join is being followed by a regular join on a non-bucketed table on the same key
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.key = a.key;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.key = a.key;

-- A SMB join is being followed by a regular join on a bucketed table on the same key
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl3 c on c.key = a.key;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl3 c on c.key = a.key;

-- A SMB join is being followed by a regular join on a bucketed table on a different key
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl4 c on c.value = a.value;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl4 c on c.value = a.value;
