create table mp2 (a string) partitioned by (b string);
alter table mp2 add partition (b='1');
alter table mp2 partition (b='1') enable NO_DROP;
alter table mp2 drop partition (b='1');
