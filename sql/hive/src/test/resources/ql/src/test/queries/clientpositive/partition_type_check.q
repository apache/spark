set hive.typecheck.on.insert = true;

-- begin part(string, string) pass(string, int)
CREATE TABLE tab1 (id1 int,id2 string) PARTITIONED BY(month string,day string) stored as textfile;
LOAD DATA LOCAL INPATH '../data/files/T1.txt' overwrite into table tab1 PARTITION(month='June', day=2);

select * from tab1;
drop table tab1;

-- begin part(string, int) pass(string, string)
CREATE TABLE tab1 (id1 int,id2 string) PARTITIONED BY(month string,day int) stored as textfile;
LOAD DATA LOCAL INPATH '../data/files/T1.txt' overwrite into table tab1 PARTITION(month='June', day='2');

select * from tab1;
drop table tab1;

-- begin part(string, date) pass(string, date)
create table tab1 (id1 int, id2 string) PARTITIONED BY(month string,day date) stored as textfile;
alter table tab1 add partition (month='June', day='2008-01-01');
LOAD DATA LOCAL INPATH '../data/files/T1.txt' overwrite into table tab1 PARTITION(month='June', day='2008-01-01');

select id1, id2, day from tab1 where day='2008-01-01';
drop table tab1;

