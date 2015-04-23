CREATE TABLE sourceTable (one string,two string) PARTITIONED BY (ds string,hr string);

load data local inpath '../../data/files/kv1.txt' INTO TABLE sourceTable partition(ds='2011-11-11', hr='11');

load data local inpath '../../data/files/kv3.txt' INTO TABLE sourceTable partition(ds='2011-11-11', hr='12');

CREATE TABLE destinTable (one string,two string) PARTITIONED BY (ds string,hr string);

EXPLAIN INSERT OVERWRITE TABLE destinTable PARTITION (ds='2011-11-11', hr='11') if not exists
SELECT one,two FROM sourceTable WHERE ds='2011-11-11' AND hr='11' order by one desc, two desc limit 5;

INSERT OVERWRITE TABLE destinTable PARTITION (ds='2011-11-11', hr='11') if not exists
SELECT one,two FROM sourceTable WHERE ds='2011-11-11' AND hr='11' order by one desc, two desc limit 5;

select one,two from destinTable order by one desc, two desc;

EXPLAIN INSERT OVERWRITE TABLE destinTable PARTITION (ds='2011-11-11', hr='11') if not exists
SELECT one,two FROM sourceTable WHERE ds='2011-11-11' AND hr='12' order by one desc, two desc limit 5;

INSERT OVERWRITE TABLE destinTable PARTITION (ds='2011-11-11', hr='11') if not exists
SELECT one,two FROM sourceTable WHERE ds='2011-11-11' AND hr='12' order by one desc, two desc limit 5;

select one,two from destinTable order by one desc, two desc;

drop table destinTable;

CREATE TABLE destinTable (one string,two string);

EXPLAIN INSERT OVERWRITE TABLE destinTable SELECT one,two FROM sourceTable WHERE ds='2011-11-11' AND hr='11' order by one desc, two desc limit 5;

INSERT OVERWRITE TABLE destinTable SELECT one,two FROM sourceTable WHERE ds='2011-11-11' AND hr='11' order by one desc, two desc limit 5;

drop table destinTable;

drop table sourceTable;
