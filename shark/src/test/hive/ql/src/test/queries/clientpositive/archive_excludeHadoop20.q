set hive.archive.enabled = true;
set hive.enforce.bucketing = true;

-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.20, 0.20S)

drop table tstsrc;
drop table tstsrcpart;

create table tstsrc like src;
insert overwrite table tstsrc select key, value from src;

create table tstsrcpart (key string, value string) partitioned by (ds string, hr string) clustered by (key) into 10 buckets;

insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='11')
select key, value from srcpart where ds='2008-04-08' and hr='11';

insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='12')
select key, value from srcpart where ds='2008-04-08' and hr='12';

insert overwrite table tstsrcpart partition (ds='2008-04-09', hr='11')
select key, value from srcpart where ds='2008-04-09' and hr='11';

insert overwrite table tstsrcpart partition (ds='2008-04-09', hr='12')
select key, value from srcpart where ds='2008-04-09' and hr='12';

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM tstsrcpart WHERE ds='2008-04-08') subq1) subq2;

ALTER TABLE tstsrcpart ARCHIVE PARTITION (ds='2008-04-08', hr='12');

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM tstsrcpart WHERE ds='2008-04-08') subq1) subq2;

SELECT key, count(1) FROM tstsrcpart WHERE ds='2008-04-08' AND hr='12' AND key='0' GROUP BY key;

SELECT * FROM tstsrcpart a JOIN tstsrc b ON a.key=b.key
WHERE a.ds='2008-04-08' AND a.hr='12' AND a.key='0';

ALTER TABLE tstsrcpart UNARCHIVE PARTITION (ds='2008-04-08', hr='12');

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM tstsrcpart WHERE ds='2008-04-08') subq1) subq2;

CREATE TABLE harbucket(key INT)
PARTITIONED by (ds STRING)
CLUSTERED BY (key) INTO 10 BUCKETS;

INSERT OVERWRITE TABLE harbucket PARTITION(ds='1') SELECT CAST(key AS INT) AS a FROM tstsrc WHERE key < 50;

SELECT key FROM harbucket TABLESAMPLE(BUCKET 1 OUT OF 10) SORT BY key;
ALTER TABLE tstsrcpart ARCHIVE PARTITION (ds='2008-04-08', hr='12');
SELECT key FROM harbucket TABLESAMPLE(BUCKET 1 OUT OF 10) SORT BY key;
ALTER TABLE tstsrcpart UNARCHIVE PARTITION (ds='2008-04-08', hr='12');
SELECT key FROM harbucket TABLESAMPLE(BUCKET 1 OUT OF 10) SORT BY key;


CREATE TABLE old_name(key INT)
PARTITIONED by (ds STRING);

INSERT OVERWRITE TABLE old_name PARTITION(ds='1') SELECT CAST(key AS INT) AS a FROM tstsrc WHERE key < 50;
ALTER TABLE old_name ARCHIVE PARTITION (ds='1');
SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM old_name WHERE ds='1') subq1) subq2;
ALTER TABLE old_name RENAME TO new_name;
SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM new_name WHERE ds='1') subq1) subq2;

drop table tstsrc;
drop table tstsrcpart;
