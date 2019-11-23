set hive.execution.engine=tez;
CREATE TABLE tab_part (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE t1 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE t1 partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE t1 partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE t1 partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE t1 partition(ds='2008-04-08');

set hive.enforce.bucketing=true;
set hive.enforce.sorting = true;
set hive.optimize.bucketingsorting=false;
set hive.stats.dbclass=fs;

insert overwrite table tab_part partition (ds='2008-04-08')
select key,value from t1;
describe formatted tab_part partition(ds='2008-04-08');

set hive.stats.dbclass=jdbc:derby;
