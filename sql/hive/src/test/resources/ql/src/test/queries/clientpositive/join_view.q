drop table invites;
drop table invites2;
create table invites (foo int, bar string) partitioned by (ds string);
create table invites2 (foo int, bar string) partitioned by (ds string);

set hive.mapred.mode=strict;

-- test join views: see HIVE-1989

create view v as select invites.bar, invites2.foo, invites2.ds from invites join invites2 on invites.ds=invites2.ds;

explain select * from v where ds='2011-09-01';

drop view v;
drop table invites;
drop table invites2;