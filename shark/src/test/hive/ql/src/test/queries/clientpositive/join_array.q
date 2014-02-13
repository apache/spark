create table tinyA(a bigint, b bigint) stored as textfile;
create table tinyB(a bigint, bList array<int>) stored as textfile;

load data local inpath '../data/files/tiny_a.txt' into table tinyA;
load data local inpath '../data/files/tiny_b.txt' into table tinyB;

select * from tinyA;
select * from tinyB;

select tinyB.a, tinyB.bList from tinyB full outer join tinyA on tinyB.a = tinyA.a;
