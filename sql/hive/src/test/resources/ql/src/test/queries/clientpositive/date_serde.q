drop table if exists date_serde_regex;
drop table date_serde_lb;
drop table date_serde_ls;
drop table date_serde_c;
drop table date_serde_lbc;
drop table date_serde_orc;


--
-- RegexSerDe
--
create table date_serde_regex (
  ORIGIN_CITY_NAME string,
  DEST_CITY_NAME string,
  FL_DATE date,
  ARR_DELAY float,
  FL_NUM int
)
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties (
  "input.regex" = "([^]*)([^]*)([^]*)([^]*)([0-9]*)"
)
stored as textfile;

load data local inpath '../data/files/flights_tiny.txt.1' overwrite into table date_serde_regex;

select * from date_serde_regex;
select fl_date, count(*) from date_serde_regex group by fl_date;

--
-- LazyBinary
--
create table date_serde_lb (
  c1 date,
  c2 int
);
alter table date_serde_lb set serde 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe';

insert overwrite table date_serde_lb 
  select fl_date, fl_num from date_serde_regex limit 1;

select * from date_serde_lb;
select c1, sum(c2) from date_serde_lb group by c1;

--
-- LazySimple
--
create table date_serde_ls (
  c1 date,
  c2 int
);
alter table date_serde_ls set serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

insert overwrite table date_serde_ls 
  select c1, c2 from date_serde_lb limit 1;

select * from date_serde_ls;
select c1, sum(c2) from date_serde_ls group by c1;

--
-- Columnar
--
create table date_serde_c (
  c1 date,
  c2 int
) stored as rcfile;
alter table date_serde_c set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';

insert overwrite table date_serde_c 
  select c1, c2 from date_serde_ls limit 1;

select * from date_serde_c;
select c1, sum(c2) from date_serde_c group by c1;

--
-- LazyBinaryColumnar
--
create table date_serde_lbc (
  c1 date,
  c2 int
) stored as rcfile;
alter table date_serde_lbc set serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';

insert overwrite table date_serde_lbc 
  select c1, c2 from date_serde_c limit 1;

select * from date_serde_lbc;
select c1, sum(c2) from date_serde_lbc group by c1;

--
-- ORC
--
create table date_serde_orc (
  c1 date,
  c2 int
) stored as orc;
alter table date_serde_orc set serde 'org.apache.hadoop.hive.ql.io.orc.OrcSerde';

insert overwrite table date_serde_orc 
  select c1, c2 from date_serde_lbc limit 1;

select * from date_serde_orc;
select c1, sum(c2) from date_serde_orc group by c1;



drop table date_serde_regex;
drop table date_serde_lb;
drop table date_serde_ls;
drop table date_serde_c;
drop table date_serde_lbc;
drop table date_serde_orc;
