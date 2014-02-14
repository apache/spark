drop table table1;
drop table table2;

set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;

create table table1(key string, value string) clustered by (key, value)
sorted by (key, value) into 1 BUCKETS stored as textfile;
create table table2(key string, value string) clustered by (value, key)
sorted by (value, key) into 1 BUCKETS stored as textfile;

load data local inpath '../data/files/SortCol1Col2.txt' overwrite into table table1;
load data local inpath '../data/files/SortCol2Col1.txt' overwrite into table table2;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;

-- The tables are bucketed in same columns in different order,
-- but sorted in different column orders
-- Neither bucketed map-join, nor sort-merge join should be performed

explain extended
select /*+ mapjoin(b) */ count(*) from table1 a join table2 b on a.key=b.key and a.value=b.value;

select /*+ mapjoin(b) */ count(*) from table1 a join table2 b on a.key=b.key and a.value=b.value;

