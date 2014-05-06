set datanucleus.cache.collections=false;
set hive.stats.autogather=true;
set hive.stats.reliable=true;
set hive.stats.dbclass=dummy;
set hive.stats.default.publisher=org.apache.hadoop.hive.ql.stats.DummyStatsPublisher;
set hive.stats.default.aggregator=org.apache.hadoop.hive.ql.stats.KeyVerifyingStatsAggregator;

-- Note, its important that the partitions created below have a name greater than 16 characters in
-- length since KeyVerifyingStatsAggregator depends on checking that a keyPrefix is hashed by the
-- length of the keyPrefix, having a partition name greather than 16 characters guarantees no false
-- positives.

create table stats_part like srcpart;

set hive.stats.key.prefix.max.length=0;

-- The stats key should be hashed since the max length is too small
insert overwrite table stats_part partition (ds='2010-04-08', hr = '13') select key, value from src;

set hive.stats.key.prefix.max.length=200;

-- The stats key should not be hashed since the max length is large enough
insert overwrite table stats_part partition (ds='2010-04-08', hr = '13') select key, value from src;

set hive.stats.key.prefix.max.length=-1;

-- The stats key should not be hashed since negative values should imply hashing is turned off
insert overwrite table stats_part partition (ds='2010-04-08', hr = '13') select key, value from src;

set hive.stats.dbclass=jdbc:derby;
set hive.stats.default.publisher=;
set hive.stats.default.aggregator=;

set hive.stats.key.prefix.max.length=0;

-- Run the tests again and verify the stats are correct, this should verify that the stats publisher
-- is hashing as well where appropriate

-- The stats key should be hashed since the max length is too small
insert overwrite table stats_part partition (ds='2010-04-08', hr = '13') select key, value from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');

set hive.stats.key.prefix.max.length=200;

-- The stats key should not be hashed since the max length is large enough
insert overwrite table stats_part partition (ds='2010-04-08', hr = '13') select key, value from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');

set hive.stats.key.prefix.max.length=-1;

-- The stats key should not be hashed since negative values should imply hashing is turned off
insert overwrite table stats_part partition (ds='2010-04-08', hr = '13') select key, value from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');


set hive.stats.dbclass=dummy;
set hive.stats.default.publisher=org.apache.hadoop.hive.ql.stats.DummyStatsPublisher;
set hive.stats.default.aggregator=org.apache.hadoop.hive.ql.stats.KeyVerifyingStatsAggregator;
set hive.stats.key.prefix.max.length=0;

-- Do the same for dynamic partitions

-- The stats key should be hashed since the max length is too small
insert overwrite table stats_part partition (ds='2010-04-08', hr) select key, value, '13' from src;

set hive.stats.key.prefix.max.length=200;

-- The stats key should not be hashed since the max length is large enough
insert overwrite table stats_part partition (ds='2010-04-08', hr) select key, value, '13' from src;

set hive.stats.key.prefix.max.length=-1;

-- The stats key should not be hashed since negative values should imply hashing is turned off
insert overwrite table stats_part partition (ds='2010-04-08', hr) select key, value, '13' from src;

set hive.stats.dbclass=jdbc:derby;
set hive.stats.default.publisher=;
set hive.stats.default.aggregator=;

set hive.stats.key.prefix.max.length=0;

-- Run the tests again and verify the stats are correct, this should verify that the stats publisher
-- is hashing as well where appropriate

-- The stats key should be hashed since the max length is too small
insert overwrite table stats_part partition (ds='2010-04-08', hr) select key, value, '13' from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');

set hive.stats.key.prefix.max.length=200;

-- The stats key should not be hashed since the max length is large enough
insert overwrite table stats_part partition (ds='2010-04-08', hr) select key, value, '13' from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');

set hive.stats.key.prefix.max.length=-1;

-- The stats key should not be hashed since negative values should imply hashing is turned off
insert overwrite table stats_part partition (ds='2010-04-08', hr) select key, value, '13' from src;

desc formatted stats_part partition (ds='2010-04-08', hr = '13');
