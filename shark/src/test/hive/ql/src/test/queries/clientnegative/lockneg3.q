drop table tstsrcpart;
create table tstsrcpart like srcpart;

insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='11')
select key, value from srcpart where ds='2008-04-08' and hr='11';

set hive.lock.numretries=0;
set hive.unlock.numretries=0;
UNLOCK TABLE tstsrcpart PARTITION(ds='2008-04-08', hr='11');
