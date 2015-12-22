drop table if exists char_serde_regex;
drop table if exists char_serde_lb;
drop table if exists char_serde_ls;
drop table if exists char_serde_c;
drop table if exists char_serde_lbc;
drop table if exists char_serde_orc;

--
-- RegexSerDe
--
create table  char_serde_regex (
  key char(15),
  value char(20)
)
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties (
  "input.regex" = "([^]*)([^]*)"
)
stored as textfile;

load data local inpath '../../data/files/srcbucket0.txt' overwrite into table char_serde_regex;

select * from char_serde_regex limit 5;
select value, count(*) from char_serde_regex group by value limit 5;

--
-- LazyBinary
--
create table  char_serde_lb (
  key char(15),
  value char(20)
);
alter table char_serde_lb set serde 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe';

insert overwrite table char_serde_lb
  select key, value from char_serde_regex;
select * from char_serde_lb limit 5;
select value, count(*) from char_serde_lb group by value limit 5;

--
-- LazySimple
--
create table  char_serde_ls (
  key char(15),
  value char(20)
);
alter table char_serde_ls set serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

insert overwrite table char_serde_ls
  select key, value from char_serde_lb;
select * from char_serde_ls limit 5;
select value, count(*) from char_serde_ls group by value limit 5;

--
-- Columnar
--
create table  char_serde_c (
  key char(15),
  value char(20)
) stored as rcfile;
alter table char_serde_c set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';

insert overwrite table char_serde_c
  select key, value from char_serde_ls;
select * from char_serde_c limit 5;
select value, count(*) from char_serde_c group by value limit 5;

--
-- LazyBinaryColumnar
--
create table char_serde_lbc (
  key char(15),
  value char(20)
) stored as rcfile;
alter table char_serde_lbc set serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';

insert overwrite table char_serde_lbc
  select key, value from char_serde_c;
select * from char_serde_lbc limit 5;
select value, count(*) from char_serde_lbc group by value limit 5;

--
-- ORC
--
create table char_serde_orc (
  key char(15),
  value char(20)
) stored as orc;
alter table char_serde_orc set serde 'org.apache.hadoop.hive.ql.io.orc.OrcSerde';


insert overwrite table char_serde_orc
  select key, value from char_serde_lbc;
select * from char_serde_orc limit 5;
select value, count(*) from char_serde_orc group by value limit 5;

drop table if exists char_serde_regex;
drop table if exists char_serde_lb;
drop table if exists char_serde_ls;
drop table if exists char_serde_c;
drop table if exists char_serde_lbc;
drop table if exists char_serde_orc;
