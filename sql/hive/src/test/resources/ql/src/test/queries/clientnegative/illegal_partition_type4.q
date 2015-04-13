create table tab1(s string) PARTITIONED BY(dt date, st string);
alter table tab1 add partition (dt=date 'foo', st='foo');
drop table tab1;
