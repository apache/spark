set hive.table.parameters.default=p1=v1,P2=v21=v22=v23;
CREATE TABLE table_p1 (a STRING);
DESC EXTENDED table_p1;

set hive.table.parameters.default=p3=v3;
CREATE TABLE table_p2 LIKE table_p1;
DESC EXTENDED table_p2;

CREATE TABLE table_p3 AS SELECT * FROM table_p1;
DESC EXTENDED table_p3;
