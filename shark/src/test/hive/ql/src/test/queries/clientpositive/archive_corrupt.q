USE default;

set hive.archive.enabled = true;
set hive.enforce.bucketing = true;

drop table tstsrcpart;

create table tstsrcpart like srcpart;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.20)
-- The version of GzipCodec provided in Hadoop 0.20 silently ignores
-- file format errors. However, versions of Hadoop that include
-- HADOOP-6835 (e.g. 0.23 and 1.x) cause a Wrong File Format exception
-- to be thrown during the LOAD step. This behavior is now tested in
-- clientnegative/archive_corrupt.q

load data local inpath '../data/files/archive_corrupt.rc' overwrite into table tstsrcpart partition (ds='2008-04-08', hr='11');

insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='12')
select key, value from srcpart where ds='2008-04-08' and hr='12';

insert overwrite table tstsrcpart partition (ds='2008-04-09', hr='11')
select key, value from srcpart where ds='2008-04-09' and hr='11';

insert overwrite table tstsrcpart partition (ds='2008-04-09', hr='12')
select key, value from srcpart where ds='2008-04-09' and hr='12';

describe extended tstsrcpart partition (ds='2008-04-08', hr='11');

alter table tstsrcpart archive partition (ds='2008-04-08', hr='11');

describe extended tstsrcpart partition (ds='2008-04-08', hr='11');

alter table tstsrcpart unarchive partition (ds='2008-04-08', hr='11');

