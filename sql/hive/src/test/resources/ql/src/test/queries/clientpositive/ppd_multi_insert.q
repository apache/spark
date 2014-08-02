set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

CREATE TABLE mi1(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE mi2(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE mi3(key INT) PARTITIONED BY(ds STRING, hr STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src a JOIN src b ON (a.key = b.key)
INSERT OVERWRITE TABLE mi1 SELECT a.* WHERE a.key < 100
INSERT OVERWRITE TABLE mi2 SELECT a.key, a.value WHERE a.key >= 100 and a.key < 200
INSERT OVERWRITE TABLE mi3 PARTITION(ds='2008-04-08', hr='12') SELECT a.key WHERE a.key >= 200 and a.key < 300
INSERT OVERWRITE DIRECTORY '../build/ql/test/data/warehouse/mi4.out' SELECT a.value WHERE a.key >= 300;

FROM src a JOIN src b ON (a.key = b.key)
INSERT OVERWRITE TABLE mi1 SELECT a.* WHERE a.key < 100
INSERT OVERWRITE TABLE mi2 SELECT a.key, a.value WHERE a.key >= 100 and a.key < 200
INSERT OVERWRITE TABLE mi3 PARTITION(ds='2008-04-08', hr='12') SELECT a.key WHERE a.key >= 200 and a.key < 300
INSERT OVERWRITE DIRECTORY '../build/ql/test/data/warehouse/mi4.out' SELECT a.value WHERE a.key >= 300;

SELECT mi1.* FROM mi1;
SELECT mi2.* FROM mi2;
SELECT mi3.* FROM mi3;
dfs -cat ../build/ql/test/data/warehouse/mi4.out/*;


set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
FROM src a JOIN src b ON (a.key = b.key)
INSERT OVERWRITE TABLE mi1 SELECT a.* WHERE a.key < 100
INSERT OVERWRITE TABLE mi2 SELECT a.key, a.value WHERE a.key >= 100 and a.key < 200
INSERT OVERWRITE TABLE mi3 PARTITION(ds='2008-04-08', hr='12') SELECT a.key WHERE a.key >= 200 and a.key < 300
INSERT OVERWRITE DIRECTORY '../build/ql/test/data/warehouse/mi4.out' SELECT a.value WHERE a.key >= 300;

FROM src a JOIN src b ON (a.key = b.key)
INSERT OVERWRITE TABLE mi1 SELECT a.* WHERE a.key < 100
INSERT OVERWRITE TABLE mi2 SELECT a.key, a.value WHERE a.key >= 100 and a.key < 200
INSERT OVERWRITE TABLE mi3 PARTITION(ds='2008-04-08', hr='12') SELECT a.key WHERE a.key >= 200 and a.key < 300
INSERT OVERWRITE DIRECTORY '../build/ql/test/data/warehouse/mi4.out' SELECT a.value WHERE a.key >= 300;

SELECT mi1.* FROM mi1;
SELECT mi2.* FROM mi2;
SELECT mi3.* FROM mi3;
dfs -cat ../build/ql/test/data/warehouse/mi4.out/*;
