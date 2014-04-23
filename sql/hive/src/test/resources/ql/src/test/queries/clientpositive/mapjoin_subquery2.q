drop table x;
drop table y;
drop table z;

CREATE TABLE x (name STRING, id INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE y (id INT, name STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE z (id INT, name STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

load data local inpath '../data/files/x.txt' INTO TABLE x;
load data local inpath '../data/files/y.txt' INTO TABLE y;
load data local inpath '../data/files/z.txt' INTO TABLE z;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

-- Since the inputs are small, it should be automatically converted to mapjoin

EXPLAIN
SELECT subq.key1, subq.value1, subq.key2, subq.value2, z.id, z.name
FROM
(SELECT x.id as key1, x.name as value1, y.id as key2, y.name as value2 
 FROM y JOIN x ON (x.id = y.id)) subq
 JOIN z ON (subq.key1 = z.id);

SELECT subq.key1, subq.value1, subq.key2, subq.value2, z.id, z.name
FROM
(SELECT x.id as key1, x.name as value1, y.id as key2, y.name as value2 
 FROM y JOIN x ON (x.id = y.id)) subq
 JOIN z ON (subq.key1 = z.id);

drop table x;
drop table y;
drop table z;
