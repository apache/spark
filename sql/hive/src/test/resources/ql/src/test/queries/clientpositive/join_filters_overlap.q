-- HIVE-3411 Filter predicates on outer join overlapped on single alias is not handled properly

create table a as SELECT 100 as key, a.value as value FROM src LATERAL VIEW explode(array(40, 50, 60)) a as value limit 3;

-- overlap on a
explain extended select * from a left outer join a b on (a.key=b.key AND a.value=50 AND b.value=50) left outer join a c on (a.key=c.key AND a.value=60 AND c.value=60);
select * from a left outer join a b on (a.key=b.key AND a.value=50 AND b.value=50) left outer join a c on (a.key=c.key AND a.value=60 AND c.value=60);
select /*+ MAPJOIN(b,c)*/ * from a left outer join a b on (a.key=b.key AND a.value=50 AND b.value=50) left outer join a c on (a.key=c.key AND a.value=60 AND c.value=60) order by a.key ASC, a.value ASC;

-- overlap on b
explain extended select * from a right outer join a b on (a.key=b.key AND a.value=50 AND b.value=50) left outer join a c on (b.key=c.key AND b.value=60 AND c.value=60);
select * from a right outer join a b on (a.key=b.key AND a.value=50 AND b.value=50) left outer join a c on (b.key=c.key AND b.value=60 AND c.value=60);
select /*+ MAPJOIN(a,c)*/ * from a right outer join a b on (a.key=b.key AND a.value=50 AND b.value=50) left outer join a c on (b.key=c.key AND b.value=60 AND c.value=60) order by b.key ASC, b.value ASC;

-- overlap on b with two filters for each
explain extended select * from a right outer join a b on (a.key=b.key AND a.value=50 AND b.value=50 AND b.value>10) left outer join a c on (b.key=c.key AND b.value=60 AND b.value>20 AND c.value=60);
select * from a right outer join a b on (a.key=b.key AND a.value=50 AND b.value=50 AND b.value>10) left outer join a c on (b.key=c.key AND b.value=60 AND b.value>20 AND c.value=60);
select /*+ MAPJOIN(a,c)*/ * from a right outer join a b on (a.key=b.key AND a.value=50 AND b.value=50 AND b.value>10) left outer join a c on (b.key=c.key AND b.value=60 AND b.value>20 AND c.value=60) order by b.key ASC, b.value ASC;

-- overlap on a, b
explain extended select * from a full outer join a b on (a.key=b.key AND a.value=50 AND b.value=50) left outer join a c on (b.key=c.key AND b.value=60 AND c.value=60) left outer join a d on (a.key=d.key AND a.value=40 AND d.value=40);
select * from a full outer join a b on (a.key=b.key AND a.value=50 AND b.value=50) left outer join a c on (b.key=c.key AND b.value=60 AND c.value=60) left outer join a d on (a.key=d.key AND a.value=40 AND d.value=40);

-- triple overlap on a
explain extended select * from a left outer join a b on (a.key=b.key AND a.value=50 AND b.value=50) left outer join a c on (a.key=c.key AND a.value=60 AND c.value=60) left outer join a d on (a.key=d.key AND a.value=40 AND d.value=40);
select * from a left outer join a b on (a.key=b.key AND a.value=50 AND b.value=50) left outer join a c on (a.key=c.key AND a.value=60 AND c.value=60) left outer join a d on (a.key=d.key AND a.value=40 AND d.value=40);
select /*+ MAPJOIN(b,c, d)*/ * from a left outer join a b on (a.key=b.key AND a.value=50 AND b.value=50) left outer join a c on (a.key=c.key AND a.value=60 AND c.value=60) left outer join a d on (a.key=d.key AND a.value=40 AND d.value=40) order by a.key ASC, a.value ASC;
