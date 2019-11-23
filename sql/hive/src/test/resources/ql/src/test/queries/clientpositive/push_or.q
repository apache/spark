drop table push_or;

create table push_or (key int, value string) partitioned by (ds string);

insert overwrite table push_or partition (ds='2000-04-08') select * from src where key < 20 order by key;
insert overwrite table push_or partition (ds='2000-04-09') select * from src where key < 20 order by key;

explain extended select key, value, ds from push_or where ds='2000-04-09' or key=5 order by key, ds;
select key, value, ds from push_or where ds='2000-04-09' or key=5 order by key, ds;

