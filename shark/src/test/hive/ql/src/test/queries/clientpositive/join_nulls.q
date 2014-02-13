CREATE TABLE myinput1(key int, value int);
LOAD DATA LOCAL INPATH '../data/files/in1.txt' INTO TABLE myinput1;

SELECT * FROM myinput1 a JOIN myinput1 b ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key=b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key and a.value=b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key=b.key and a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.value = b.value and a.key=b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT * from myinput1 a LEFT OUTER JOIN myinput1 b ON (a.value=b.value) RIGHT OUTER JOIN myinput1 c ON (b.value=c.value) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * from myinput1 a RIGHT OUTER JOIN myinput1 b ON (a.value=b.value) LEFT OUTER JOIN myinput1 c ON (b.value=c.value) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b RIGHT OUTER JOIN myinput1 c ON a.value = b.value and b.value = c.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

CREATE TABLE smb_input1(key int, value int) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS; 
CREATE TABLE smb_input2(key int, value int) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS; 
LOAD DATA LOCAL INPATH '../data/files/in1.txt' into table smb_input1;
LOAD DATA LOCAL INPATH '../data/files/in2.txt' into table smb_input1;
LOAD DATA LOCAL INPATH '../data/files/in1.txt' into table smb_input2;
LOAD DATA LOCAL INPATH '../data/files/in2.txt' into table smb_input2;

SET hive.optimize.bucketmapJOIN = true;
SET hive.optimize.bucketmapJOIN.sortedmerge = true;
SET hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key = b.key AND a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a RIGHT OUTER JOIN smb_input1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a LEFT OUTER JOIN smb_input1 b ON a.key = b.key ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input2 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a JOIN smb_input2 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a LEFT OUTER JOIN smb_input2 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a RIGHT OUTER JOIN smb_input2 b ON a.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input2 a JOIN smb_input2 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2 a RIGHT OUTER JOIN smb_input2 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a JOIN smb_input2 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a LEFT OUTER JOIN smb_input2 b ON a.value = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
