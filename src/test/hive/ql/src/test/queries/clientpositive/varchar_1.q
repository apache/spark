drop table varchar1;
drop table varchar1_1;

create table varchar1 (key varchar(10), value varchar(20));
create table varchar1_1 (key string, value string);

-- load from file
load data local inpath '../data/files/srcbucket0.txt' overwrite into table varchar1;
select * from varchar1 order by key, value limit 2;

-- insert overwrite, from same/different length varchar
insert overwrite table varchar1
  select cast(key as varchar(10)), cast(value as varchar(15)) from src order by key, value limit 2;
select key, value from varchar1 order by key, value;

-- insert overwrite, from string
insert overwrite table varchar1
  select key, value from src order by key, value limit 2;
select key, value from varchar1 order by key, value;

-- insert string from varchar
insert overwrite table varchar1_1
  select key, value from varchar1 order by key, value limit 2;
select key, value from varchar1_1 order by key, value;

-- respect string length
insert overwrite table varchar1 
  select key, cast(value as varchar(3)) from src order by key, value limit 2;
select key, value from varchar1 order by key, value;

drop table varchar1;
drop table varchar1_1;
