set hive.stats.autogather=false;

drop table stats16;

create table stats16 (key int, value string);
desc formatted stats16;

insert into table stats16 select * from src;
analyze table stats16 compute statistics;
desc formatted stats16;

drop table stats16;
