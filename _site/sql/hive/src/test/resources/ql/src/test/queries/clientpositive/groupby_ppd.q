-- see HIVE-2382
create table invites (id int, foo int, bar int);
explain select * from (select foo, bar from (select bar, foo from invites c union all select bar, foo from invites d) b) a group by bar, foo having bar=1;
drop table invites;