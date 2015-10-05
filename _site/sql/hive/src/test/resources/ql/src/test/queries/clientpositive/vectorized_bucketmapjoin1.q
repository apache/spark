create table vsmb_bucket_1(key int, value string) 
  CLUSTERED BY (key) 
  SORTED BY (key) INTO 1 BUCKETS 
  STORED AS ORC;
create table vsmb_bucket_2(key int, value string) 
  CLUSTERED BY (key) 
  SORTED BY (key) INTO 1 BUCKETS 
  STORED AS ORC;

create table vsmb_bucket_RC(key int, value string) 
  CLUSTERED BY (key) 
  SORTED BY (key) INTO 1 BUCKETS 
  STORED AS RCFILE;

create table vsmb_bucket_TXT(key int, value string) 
  CLUSTERED BY (key) 
  SORTED BY (key) INTO 1 BUCKETS 
  STORED AS TEXTFILE;
  
insert into table vsmb_bucket_1 select cint, cstring1 from alltypesorc limit 2;
insert into table vsmb_bucket_2 select cint, cstring1 from alltypesorc limit 2;
insert into table vsmb_bucket_RC select cint, cstring1 from alltypesorc limit 2;
insert into table vsmb_bucket_TXT select cint, cstring1 from alltypesorc limit 2;  

set hive.vectorized.execution.enabled=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.sortmerge.join.noconditionaltask = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

explain
select /*+MAPJOIN(a)*/ * from vsmb_bucket_1 a join vsmb_bucket_2 b on a.key = b.key;
select /*+MAPJOIN(a)*/ * from vsmb_bucket_1 a join vsmb_bucket_2 b on a.key = b.key;

explain
select /*+MAPJOIN(b)*/ * from vsmb_bucket_1 a join vsmb_bucket_RC b on a.key = b.key;
select /*+MAPJOIN(b)*/ * from vsmb_bucket_1 a join vsmb_bucket_RC b on a.key = b.key;

-- RC file does not yet provide the vectorized CommonRCFileformat out-of-the-box
-- explain
-- select /*+MAPJOIN(b)*/ * from vsmb_bucket_RC a join vsmb_bucket_2 b on a.key = b.key;
-- select /*+MAPJOIN(b)*/ * from vsmb_bucket_RC a join vsmb_bucket_2 b on a.key = b.key;

explain
select /*+MAPJOIN(b)*/ * from vsmb_bucket_1 a join vsmb_bucket_TXT b on a.key = b.key;
select /*+MAPJOIN(b)*/ * from vsmb_bucket_1 a join vsmb_bucket_TXT b on a.key = b.key;
