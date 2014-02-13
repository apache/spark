set hive.archive.enabled = true;
-- Tests trying to (possible) dynamic insert into archived partition.
-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.17, 0.18, 0.19)

CREATE TABLE tstsrcpart LIKE srcpart;

INSERT OVERWRITE TABLE tstsrcpart PARTITION (ds='2008-04-08', hr='12')
SELECT key, value FROM srcpart WHERE ds='2008-04-08' AND hr='12';

ALTER TABLE tstsrcpart ARCHIVE PARTITION (ds='2008-04-08', hr='12');

SET hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE tstsrcpart PARTITION (ds='2008-04-08', hr)
SELECT key, value, hr FROM srcpart WHERE ds='2008-04-08' AND hr='12';
