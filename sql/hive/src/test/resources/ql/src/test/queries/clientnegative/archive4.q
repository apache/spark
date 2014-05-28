set hive.archive.enabled = true;
-- Tests archiving multiple partitions
-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.17, 0.18, 0.19)

ALTER TABLE srcpart ARCHIVE PARTITION (ds='2008-04-08', hr='12') PARTITION (ds='2008-04-08', hr='11');
