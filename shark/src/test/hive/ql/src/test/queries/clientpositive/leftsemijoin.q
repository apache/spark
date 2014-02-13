drop table sales;
drop table things;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE sales (name STRING, id INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE things (id INT, name STRING) partitioned by (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

load data local inpath '../data/files/sales.txt' INTO TABLE sales;
load data local inpath '../data/files/things.txt' INTO TABLE things partition(ds='2011-10-23');
load data local inpath '../data/files/things2.txt' INTO TABLE things partition(ds='2011-10-24');

SELECT name,id FROM sales ORDER BY name ASC, id ASC;

SELECT id,name FROM things ORDER BY id ASC, name ASC;

SELECT name,id FROM sales LEFT SEMI JOIN things ON (sales.id = things.id) ORDER BY name ASC, id ASC;

drop table sales;
drop table things;
