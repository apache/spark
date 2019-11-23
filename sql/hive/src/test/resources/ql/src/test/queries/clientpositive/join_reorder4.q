CREATE TABLE T1(key1 STRING, val1 STRING) STORED AS TEXTFILE;
CREATE TABLE T2(key2 STRING, val2 STRING) STORED AS TEXTFILE;
CREATE TABLE T3(key3 STRING, val3 STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;
LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2;
LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3;

set hive.auto.convert.join=true;

explain select /*+ STREAMTABLE(a) */ a.*, b.*, c.* from T1 a join T2 b on a.key1=b.key2 join T3 c on a.key1=c.key3;
select /*+ STREAMTABLE(a) */ a.*, b.*, c.* from T1 a join T2 b on a.key1=b.key2 join T3 c on a.key1=c.key3;

explain select /*+ STREAMTABLE(b) */ a.*, b.*, c.* from T1 a join T2 b on a.key1=b.key2 join T3 c on a.key1=c.key3;
select /*+ STREAMTABLE(b) */ a.*, b.*, c.* from T1 a join T2 b on a.key1=b.key2 join T3 c on a.key1=c.key3;

explain select /*+ STREAMTABLE(c) */ a.*, b.*, c.* from T1 a join T2 b on a.key1=b.key2 join T3 c on a.key1=c.key3;
select /*+ STREAMTABLE(c) */ a.*, b.*, c.* from T1 a join T2 b on a.key1=b.key2 join T3 c on a.key1=c.key3;
