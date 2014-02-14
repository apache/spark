drop table tstsrc;
create table tstsrc like src;
insert overwrite table tstsrc
select key, value from src;

alter table tstsrc change src_not_exist key_value string;
