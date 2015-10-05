drop table tstsrc;
create table tstsrc like src;
insert overwrite table tstsrc select key, value from src;

set hive.lock.numretries=0;
set hive.unlock.numretries=0;

LOCK TABLE tstsrc SHARED;
LOCK TABLE tstsrc SHARED;
LOCK TABLE tstsrc EXCLUSIVE;
