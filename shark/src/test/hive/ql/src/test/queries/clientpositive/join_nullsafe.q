set hive.nullsafe.equijoin=true;

CREATE TABLE myinput1(key int, value int);
LOAD DATA LOCAL INPATH '../data/files/in8.txt' INTO TABLE myinput1;

-- merging
explain select * from myinput1 a join myinput1 b on a.key<=>b.value ORDER BY a.key, a.value, b.key, b.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value ORDER BY a.key, a.value, b.key, b.value;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key=c.key ORDER BY a.key, a.value, b.key, b.value, c.key, c.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key=c.key ORDER BY a.key, a.value, b.key, b.value, c.key, c.value;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key<=>c.key ORDER BY a.key, a.value, b.key, b.value, c.key, c.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key<=>c.key ORDER BY a.key, a.value, b.key, b.value, c.key, c.value;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value=b.key join myinput1 c on a.key<=>c.key AND a.value=c.value ORDER BY a.key, a.value, b.key, b.value, c.key, c.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value=b.key join myinput1 c on a.key<=>c.key AND a.value=c.value ORDER BY a.key, a.value, b.key, b.value, c.key, c.value;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value<=>b.key join myinput1 c on a.key<=>c.key AND a.value<=>c.value ORDER BY a.key, a.value, b.key, b.value, c.key, c.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value<=>b.key join myinput1 c on a.key<=>c.key AND a.value<=>c.value ORDER BY a.key, a.value, b.key, b.value, c.key, c.value;

-- outer joins
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key<=>b.value ORDER BY a.key, a.value, b.key, b.value;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key<=>b.value ORDER BY a.key, a.value, b.key, b.value;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key<=>b.value ORDER BY a.key, a.value, b.key, b.value;

-- map joins
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key<=>b.value ORDER BY a.key, a.value, b.key, b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key<=>b.value ORDER BY a.key, a.value, b.key, b.value;

-- smbs
CREATE TABLE smb_input1(key int, value int) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE smb_input2(key int, value int) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
LOAD DATA LOCAL INPATH '../data/files/in8.txt' into table smb_input1;
LOAD DATA LOCAL INPATH '../data/files/in9.txt' into table smb_input1;
LOAD DATA LOCAL INPATH '../data/files/in8.txt' into table smb_input2;
LOAD DATA LOCAL INPATH '../data/files/in9.txt' into table smb_input2;

SET hive.optimize.bucketmapJOIN = true;
SET hive.optimize.bucketmapJOIN.sortedmerge = true;
SET hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key <=> b.key ORDER BY a.key, a.value, b.key, b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key <=> b.key AND a.value <=> b.value ORDER BY a.key, a.value, b.key, b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a RIGHT OUTER JOIN smb_input1 b ON a.key <=> b.key ORDER BY a.key, a.value, b.key, b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key <=> b.key ORDER BY a.key, a.value, b.key, b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a LEFT OUTER JOIN smb_input1 b ON a.key <=> b.key ORDER BY a.key, a.value, b.key, b.value;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input2 b ON a.key <=> b.value ORDER BY a.key, a.value, b.key, b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a JOIN smb_input2 b ON a.key <=> b.value ORDER BY a.key, a.value, b.key, b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a LEFT OUTER JOIN smb_input2 b ON a.key <=> b.value ORDER BY a.key, a.value, b.key, b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a RIGHT OUTER JOIN smb_input2 b ON a.key <=> b.value ORDER BY a.key, a.value, b.key, b.value;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input2 a JOIN smb_input2 b ON a.value <=> b.value ORDER BY a.key, a.value, b.key, b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2 a RIGHT OUTER JOIN smb_input2 b ON a.value <=> b.value ORDER BY a.key, a.value, b.key, b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a JOIN smb_input2 b ON a.value <=> b.value ORDER BY a.key, a.value, b.key, b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a LEFT OUTER JOIN smb_input2 b ON a.value <=> b.value ORDER BY a.key, a.value, b.key, b.value;

--HIVE-3315 join predicate transitive
explain select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.key is NULL;
select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.key is NULL order by a.value ASC, b.key ASC;
