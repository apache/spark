drop table char_union1_ch1;
drop table char_union1_ch2;
drop table char_union1_str;

create table  char_union1_ch1 (
  c1 int,
  c2 char(10)
);

create table  char_union1_ch2 (
  c1 int,
  c2 char(20)
);

create table  char_union1_str (
  c1 int,
  c2 string
);

load data local inpath '../../data/files/vc1.txt' into table char_union1_ch1;
load data local inpath '../../data/files/vc1.txt' into table char_union1_ch2;
load data local inpath '../../data/files/vc1.txt' into table char_union1_str;

-- union char with same length char
select * from (
  select * from char_union1_ch1
  union all
  select * from char_union1_ch1 limit 1
) q1 sort by c1;

-- union char with different length char
select * from (
  select * from char_union1_ch1
  union all
  select * from char_union1_ch2 limit 1
) q1 sort by c1;

-- union char with string
select * from (
  select * from char_union1_ch1
  union all
  select * from char_union1_str limit 1
) q1 sort by c1;

drop table char_union1_ch1;
drop table char_union1_ch2;
drop table char_union1_str;
