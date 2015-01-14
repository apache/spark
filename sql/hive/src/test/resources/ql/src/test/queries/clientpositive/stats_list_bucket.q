
set hive.mapred.supports.subdirectories=true;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)

drop table stats_list_bucket;
drop table stats_list_bucket_1;

create table stats_list_bucket (
  c1 string,
  c2 string
) partitioned by (ds string, hr string)
skewed by (c1, c2) on  (('466','val_466'),('287','val_287'),('82','val_82'))
stored as directories
stored as rcfile;

set hive.stats.key.prefix.max.length=1;

-- Make sure we use hashed IDs during stats publishing.
-- Try partitioned table with list bucketing.
-- The stats should show 500 rows loaded, as many rows as the src table has.

insert overwrite table stats_list_bucket partition (ds = '2008-04-08',  hr = '11')
  select key, value from src;

desc formatted stats_list_bucket partition (ds = '2008-04-08',  hr = '11');

-- Also try non-partitioned table with list bucketing.
-- Stats should show the same number of rows.

create table stats_list_bucket_1 (
  c1 string,
  c2 string
)
skewed by (c1, c2) on  (('466','val_466'),('287','val_287'),('82','val_82'))
stored as directories
stored as rcfile;

insert overwrite table stats_list_bucket_1
  select key, value from src;

desc formatted stats_list_bucket_1;

drop table stats_list_bucket;
drop table stats_list_bucket_1;
