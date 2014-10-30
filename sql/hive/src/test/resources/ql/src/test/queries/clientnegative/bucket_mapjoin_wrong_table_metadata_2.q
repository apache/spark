-- Although the user has specified a bucketed map-join, the number of buckets in the table
-- do not match the number of files
drop table table1;
drop table table2;

create table table1(key string, value string) partitioned by (ds string) clustered by (key, value)
into 2 BUCKETS stored as textfile;
create table table2(key string, value string) clustered by (value, key)
into 2 BUCKETS stored as textfile;

load data local inpath '../../data/files/T1.txt' overwrite into table table1 partition (ds='1');
load data local inpath '../../data/files/T2.txt' overwrite into table table1 partition (ds='1');

load data local inpath '../../data/files/T1.txt' overwrite into table table1 partition (ds='2');

load data local inpath '../../data/files/T1.txt' overwrite into table table2;
load data local inpath '../../data/files/T2.txt' overwrite into table table2;

set hive.optimize.bucketmapjoin = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

select /*+ mapjoin(b) */ count(*) from table1 a join table2 b
on a.key=b.key and a.value=b.value and a.ds is not null;

