CREATE TABLE myinput1(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in3.txt' INTO TABLE myinput1;

SELECT * FROM myinput1 a JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT * FROM myinput1 a JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key and a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key=b.key and a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT * from myinput1 a LEFT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;
SELECT * from myinput1 a RIGHT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b RIGHT OUTER JOIN myinput1 c ON a.value = b.value and b.value = c.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;
SELECT * from myinput1 a LEFT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;
SELECT * from myinput1 a RIGHT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b RIGHT OUTER JOIN myinput1 c ON a.value = b.value and b.key = c.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;

SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

CREATE TABLE smb_input1(key int, value int) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS; 
CREATE TABLE smb_input2(key int, value int) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS; 
LOAD DATA LOCAL INPATH '../../data/files/in1.txt' into table smb_input1;
LOAD DATA LOCAL INPATH '../../data/files/in2.txt' into table smb_input1;
LOAD DATA LOCAL INPATH '../../data/files/in1.txt' into table smb_input2;
LOAD DATA LOCAL INPATH '../../data/files/in2.txt' into table smb_input2;

SET hive.optimize.bucketmapjoin = true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;
SET hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input2 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2 a JOIN smb_input2 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key = b.key AND a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a JOIN smb_input2 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a JOIN smb_input2 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a JOIN smb_input2 b ON a.key = b.key AND a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a LEFT OUTER JOIN smb_input1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a LEFT OUTER JOIN smb_input2 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a LEFT OUTER JOIN smb_input2 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a RIGHT OUTER JOIN smb_input1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a RIGHT OUTER JOIN smb_input2 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2 a RIGHT OUTER JOIN smb_input2 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SET hive.outerjoin.supports.filters = false;

SELECT * FROM myinput1 a JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT * FROM myinput1 a JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key and a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key=b.key and a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT * from myinput1 a LEFT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;
SELECT * from myinput1 a RIGHT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b RIGHT OUTER JOIN myinput1 c ON a.value = b.value and b.value = c.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;
SELECT * from myinput1 a LEFT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;
SELECT * from myinput1 a RIGHT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b RIGHT OUTER JOIN myinput1 c ON a.value = b.value and b.key = c.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC, c.key ASC, c.value ASC;

SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input2 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2 a JOIN smb_input2 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key = b.key AND a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a JOIN smb_input2 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a JOIN smb_input2 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a JOIN smb_input2 b ON a.key = b.key AND a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a LEFT OUTER JOIN smb_input1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a LEFT OUTER JOIN smb_input2 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a LEFT OUTER JOIN smb_input2 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a RIGHT OUTER JOIN smb_input1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a RIGHT OUTER JOIN smb_input2 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2 a RIGHT OUTER JOIN smb_input2 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value ASC;
