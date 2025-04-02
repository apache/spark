-- Tests the copying over of Table Parameters according to a HiveConf setting
-- when doing a CREATE TABLE LIKE.

CREATE TABLE table1(a INT, b STRING);
ALTER TABLE table1 SET TBLPROPERTIES ('a'='1', 'b'='2', 'c'='3', 'd' = '4');

SET hive.ddl.createtablelike.properties.whitelist=a,c,D;
CREATE TABLE table2 LIKE table1;
DESC FORMATTED table2;
