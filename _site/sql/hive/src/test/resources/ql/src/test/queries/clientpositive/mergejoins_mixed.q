-- HIVE-3464

create table a (key string, value string);

-- (a-b-c-d)
explain
select * from a join a b on (a.key=b.key) left outer join a c on (b.key=c.key) left outer join a d on (a.key=d.key);

explain
select * from a join a b on (a.key=b.key) left outer join a c on (b.key=c.key) right outer join a d on (a.key=d.key);

explain
select * from a join a b on (a.key=b.key) right outer join a c on (b.key=c.key) left outer join a d on (a.key=d.key);

explain
select * from a join a b on (a.key=b.key) right outer join a c on (b.key=c.key) right outer join a d on (a.key=d.key);

-- ((a-b-d)-c) (reordered)
explain
select * from a join a b on (a.key=b.key) left outer join a c on (b.value=c.key) left outer join a d on (a.key=d.key);

explain
select * from a join a b on (a.key=b.key) right outer join a c on (b.value=c.key) right outer join a d on (a.key=d.key);

explain
select * from a join a b on (a.key=b.key) full outer join a c on (b.value=c.key) full outer join a d on (a.key=d.key);

-- (((a-b)-c)-d)
explain
select * from a join a b on (a.key=b.key) left outer join a c on (b.value=c.key) right outer join a d on (a.key=d.key);

explain
select * from a join a b on (a.key=b.key) left outer join a c on (b.value=c.key) full outer join a d on (a.key=d.key);

explain
select * from a join a b on (a.key=b.key) right outer join a c on (b.value=c.key) left outer join a d on (a.key=d.key);

explain
select * from a join a b on (a.key=b.key) right outer join a c on (b.value=c.key) full outer join a d on (a.key=d.key);

-- ((a-b)-c-d)
explain
select * from a join a b on (a.key=b.key) left outer join a c on (b.value=c.key) left outer join a d on (c.key=d.key);
