set hive.archive.enabled = true;
set hive.enforce.bucketing = true;

drop table tstsrc;
drop table tstsrcpart;

create table tstsrc like src;
insert overwrite table tstsrc select key, value from src;

create table tstsrcpart like srcpart;

insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='11')
select key, value from srcpart where ds='2008-04-08' and hr='11';

insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='12')
select key, value from srcpart where ds='2008-04-08' and hr='12';

insert overwrite table tstsrcpart partition (ds='2008-04-09', hr='11')
select key, value from srcpart where ds='2008-04-09' and hr='11';

insert overwrite table tstsrcpart partition (ds='2008-04-09', hr='12')
select key, value from srcpart where ds='2008-04-09' and hr='12';

-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.17, 0.18, 0.19)

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM tstsrcpart WHERE ds='2008-04-08') subq1) subq2;

ALTER TABLE tstsrcpart ARCHIVE PARTITION (ds='2008-04-08');

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM tstsrcpart WHERE ds='2008-04-08') subq1) subq2;

SELECT key, count(1) FROM tstsrcpart WHERE ds='2008-04-08' AND hr='12' AND key='0' GROUP BY key;

SELECT * FROM tstsrcpart a JOIN tstsrc b ON a.key=b.key
WHERE a.ds='2008-04-08' AND a.hr='12' AND a.key='0';

ALTER TABLE tstsrcpart UNARCHIVE PARTITION (ds='2008-04-08');

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col
FROM (SELECT * FROM tstsrcpart WHERE ds='2008-04-08') subq1) subq2;
