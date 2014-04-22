create table tab1 (id1 int, id2 string) PARTITIONED BY(month string,day int) row format delimited fields terminated by ',';
alter table tab1 add partition (month='June', day='second');
drop table tab1;
