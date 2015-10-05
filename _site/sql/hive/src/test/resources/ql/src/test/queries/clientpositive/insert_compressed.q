set hive.exec.compress.output=true;

drop table insert_compressed;
create table insert_compressed (key int, value string);

insert overwrite table insert_compressed select * from src;
select count(*) from insert_compressed;

insert into table insert_compressed select * from src;
select count(*) from insert_compressed;

insert into table insert_compressed select * from src;
select count(*) from insert_compressed;

drop table insert_compressed;
