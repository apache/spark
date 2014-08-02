drop table varchar_union1_vc1;
drop table varchar_union1_vc2;
drop table varchar_union1_str;

create table  varchar_union1_vc1 (
  c1 int,
  c2 varchar(10)
);

create table  varchar_union1_vc2 (
  c1 int,
  c2 varchar(20)
);

create table  varchar_union1_str (
  c1 int,
  c2 string
);

load data local inpath '../data/files/vc1.txt' into table varchar_union1_vc1;
load data local inpath '../data/files/vc1.txt' into table varchar_union1_vc2;
load data local inpath '../data/files/vc1.txt' into table varchar_union1_str;

-- union varchar with same length varchar
select * from (
  select * from varchar_union1_vc1
  union all
  select * from varchar_union1_vc1 limit 1
) q1 sort by c1;

-- union varchar with different length varchar
select * from (
  select * from varchar_union1_vc1
  union all
  select * from varchar_union1_vc2 limit 1
) q1 sort by c1;

-- union varchar with string
select * from (
  select * from varchar_union1_vc1
  union all
  select * from varchar_union1_str limit 1
) q1 sort by c1;

drop table varchar_union1_vc1;
drop table varchar_union1_vc2;
drop table varchar_union1_str;
