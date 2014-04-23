set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
create table tmp1 (a string, b string) clustered by (a) sorted by (a) into 10 buckets;

create table tmp2 (a string, b string) clustered by (a) sorted by (a) into 10 buckets;


set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max=1;


insert overwrite table tmp1 select * from src where key < 50;
insert overwrite table tmp2 select * from src where key < 50;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.merge.mapfiles=false;
create table tmp3 (a string, b string, c string) clustered by (a) sorted by (a) into 10 buckets;


insert overwrite table tmp3
  select /*+ MAPJOIN(l) */ i.a, i.b, l.b
  from tmp1 i join tmp2 l ON i.a = l.a;

select * from tmp3 order by a, b, c;
