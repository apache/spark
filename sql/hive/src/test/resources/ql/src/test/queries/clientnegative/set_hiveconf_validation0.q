-- should fail: hive.join.cache.size accepts int type
desc src;

set hive.conf.validation=true;
set hive.join.cache.size=test;
