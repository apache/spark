-- begin part(string, int) pass(string, string)
CREATE TABLE tab1 (id1 int,id2 string) PARTITIONED BY(month string,day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;
LOAD DATA LOCAL INPATH '../data/files/T1.txt' overwrite into table tab1 PARTITION(month='June', day='second');

select * from tab1;
drop table tab1;

