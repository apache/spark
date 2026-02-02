
create table tmptable(key string, value string, hr string, ds string);

EXPLAIN
insert overwrite table tmptable
SELECT x.* FROM SRCPART x WHERE x.ds = '2008-04-08' and x.key < 100;

insert overwrite table tmptable
SELECT x.* FROM SRCPART x WHERE x.ds = '2008-04-08' and x.key < 100;

select * from tmptable x sort by x.key,x.value,x.ds,x.hr;

