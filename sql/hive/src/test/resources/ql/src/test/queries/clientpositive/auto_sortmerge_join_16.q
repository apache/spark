set hive.auto.convert.join=true;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;

set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;

CREATE TABLE stage_bucket_big
(
key BIGINT,
value STRING
)
PARTITIONED BY (file_tag STRING);

CREATE TABLE bucket_big
(
key BIGINT,
value STRING
)
PARTITIONED BY (day STRING, pri bigint)
clustered by (key) sorted by (key) into 12 buckets
stored as RCFile;

CREATE TABLE stage_bucket_small
(
key BIGINT,
value string
)
PARTITIONED BY (file_tag STRING);

CREATE TABLE bucket_small
(
key BIGINT,
value string
)
PARTITIONED BY (pri bigint)
clustered by (key) sorted by (key) into 12 buckets
stored as RCFile;

load data local inpath '../../data/files/srcsortbucket1outof4.txt' overwrite into table stage_bucket_small partition (file_tag='1'); 
load data local inpath '../../data/files/srcsortbucket1outof4.txt' overwrite into table stage_bucket_small partition (file_tag='2'); 

insert overwrite table bucket_small partition(pri) 
select 
key, 
value, 
file_tag as pri 
from 
stage_bucket_small 
where file_tag between 1 and 2;

load data local inpath '../../data/files/smallsrcsortbucket1outof4.txt' overwrite into table stage_bucket_big partition (file_tag='1'); 

insert overwrite table bucket_big partition(day,pri) 
select 
key, 
value, 
'day1' as day, 
1 as pri 
from 
stage_bucket_big 
where 
file_tag='1'; 

select 
a.key , 
a.value , 
b.value , 
'day1' as day, 
1 as pri 
from 
( 
select 
key, 
value 
from bucket_big where day='day1'
) a 
left outer join 
( 
select 
key, 
value
from bucket_small 
where pri between 1 and 2
) b 
on 
(a.key = b.key) 
; 

