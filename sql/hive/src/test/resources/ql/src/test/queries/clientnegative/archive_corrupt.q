USE default;

set hive.archive.enabled = true;
set hive.enforce.bucketing = true;

drop table tstsrcpart;

create table tstsrcpart like srcpart;

-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.20)
-- The version of GzipCodec that is provided in Hadoop 0.20 silently ignores
-- file format errors. However, versions of Hadoop that include
-- HADOOP-6835 (e.g. 0.23 and 1.x) cause a Wrong File Format exception
-- to be thrown during the LOAD step. This former behavior is tested
-- in clientpositive/archive_corrupt.q

load data local inpath '../data/files/archive_corrupt.rc' overwrite into table tstsrcpart partition (ds='2008-04-08', hr='11');

