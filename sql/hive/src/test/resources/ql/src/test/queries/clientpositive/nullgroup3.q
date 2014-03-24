CREATE TABLE tstparttbl(KEY STRING, VALUE STRING) PARTITIONED BY(ds string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE tstparttbl PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../data/files/nullfile.txt' INTO TABLE tstparttbl PARTITION (ds='2008-04-08');
explain
select count(1) from tstparttbl;
select count(1) from tstparttbl;

CREATE TABLE tstparttbl2(KEY STRING, VALUE STRING) PARTITIONED BY(ds string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/nullfile.txt' INTO TABLE tstparttbl2 PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../data/files/nullfile.txt' INTO TABLE tstparttbl2 PARTITION (ds='2008-04-08');
explain
select count(1) from tstparttbl2;
select count(1) from tstparttbl2;
DROP TABLE tstparttbl;
CREATE TABLE tstparttbl(KEY STRING, VALUE STRING) PARTITIONED BY(ds string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE tstparttbl PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../data/files/nullfile.txt' INTO TABLE tstparttbl PARTITION (ds='2008-04-08');
explain
select count(1) from tstparttbl;
select count(1) from tstparttbl;

DROP TABLE tstparttbl2;
CREATE TABLE tstparttbl2(KEY STRING, VALUE STRING) PARTITIONED BY(ds string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/nullfile.txt' INTO TABLE tstparttbl2 PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../data/files/nullfile.txt' INTO TABLE tstparttbl2 PARTITION (ds='2008-04-08');
explain
select count(1) from tstparttbl2;
select count(1) from tstparttbl2;
