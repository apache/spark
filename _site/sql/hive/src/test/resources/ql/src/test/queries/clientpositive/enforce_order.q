drop table table_asc;
drop table table_desc;

set hive.enforce.sorting = true;

create table table_asc(key string, value string) clustered by (key) sorted by (key ASC) into 1 BUCKETS;
create table table_desc(key string, value string) clustered by (key) sorted by (key DESC) into 1 BUCKETS;

insert overwrite table table_asc select key, value from src;
insert overwrite table table_desc select key, value from src;

select * from table_asc limit 10;
select * from table_desc limit 10;
