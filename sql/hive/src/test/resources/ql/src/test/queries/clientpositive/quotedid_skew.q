
set hive.support.quoted.identifiers=column;

set hive.mapred.supports.subdirectories=true;
set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1(`!@#$%^&*()_q` string, `y&y` string)
SKEWED BY (`!@#$%^&*()_q`) ON ((2)) STORED AS TEXTFILE
;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

CREATE TABLE T2(`!@#$%^&*()_q` string, `y&y` string)
SKEWED BY (`!@#$%^&*()_q`) ON ((2)) STORED AS TEXTFILE
;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T2;

-- a simple join query with skew on both the tables on the join key
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1 a JOIN T2 b ON a. `!@#$%^&*()_q`  = b. `!@#$%^&*()_q` 
;

