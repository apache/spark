drop table varchar_join1_vc1;
drop table varchar_join1_vc2;
drop table varchar_join1_str;

create table  varchar_join1_vc1 (
  c1 int,
  c2 varchar(10)
);

create table  varchar_join1_vc2 (
  c1 int,
  c2 varchar(20)
);

create table  varchar_join1_str (
  c1 int,
  c2 string
);

load data local inpath '../data/files/vc1.txt' into table varchar_join1_vc1;
load data local inpath '../data/files/vc1.txt' into table varchar_join1_vc2;
load data local inpath '../data/files/vc1.txt' into table varchar_join1_str;

-- Join varchar with same length varchar
select * from varchar_join1_vc1 a join varchar_join1_vc1 b on (a.c2 = b.c2) order by a.c1;

-- Join varchar with different length varchar
select * from varchar_join1_vc1 a join varchar_join1_vc2 b on (a.c2 = b.c2) order by a.c1;

-- Join varchar with string
select * from varchar_join1_vc1 a join varchar_join1_str b on (a.c2 = b.c2) order by a.c1;

drop table varchar_join1_vc1;
drop table varchar_join1_vc2;
drop table varchar_join1_str;
