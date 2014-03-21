set hive.archive.enabled = true;
-- Tests trying to unarchive a non-archived partition group
-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.17, 0.18, 0.19)

drop table tstsrcpart;
create table tstsrcpart like srcpart;
INSERT OVERWRITE TABLE tstsrcpart PARTITION (ds='2008-04-08', hr='11')
SELECT key, value FROM srcpart WHERE ds='2008-04-08' AND hr='11';
insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='12')
select key, value from srcpart where ds='2008-04-08' and hr='12';

ALTER TABLE tstsrcpart UNARCHIVE PARTITION (ds='2008-04-08', hr='12');
