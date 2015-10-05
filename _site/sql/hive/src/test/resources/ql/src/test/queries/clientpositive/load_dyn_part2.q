
create table if not exists nzhang_part_bucket (key string, value string) 
  partitioned by (ds string, hr string) 
  clustered by (key) into 10 buckets;

describe extended nzhang_part_bucket;

set hive.merge.mapfiles=false;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition=true;

explain
insert overwrite table nzhang_part_bucket partition (ds='2010-03-23', hr) select key, value, hr from srcpart where ds is not null and hr is not null;

insert overwrite table nzhang_part_bucket partition (ds='2010-03-23', hr) select key, value, hr from srcpart where ds is not null and hr is not null;

show partitions nzhang_part_bucket;

select * from nzhang_part_bucket where ds='2010-03-23' and hr='11' order by key;
select * from nzhang_part_bucket where ds='2010-03-23' and hr='12' order by key;



