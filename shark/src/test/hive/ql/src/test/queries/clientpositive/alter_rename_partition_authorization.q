-- SORT_BEFORE_DIFF

create table src_auth_tmp as select * from src;

create table authorization_part (key int, value string) partitioned by (ds string);
ALTER TABLE authorization_part SET TBLPROPERTIES ("PARTITION_LEVEL_PRIVILEGE"="TRUE");
set hive.security.authorization.enabled=true;
grant select on table src_auth_tmp to user hive_test_user;

-- column grant to user
grant Create on table authorization_part to user hive_test_user;
grant Update on table authorization_part to user hive_test_user;
grant Drop on table authorization_part to user hive_test_user;

show grant user hive_test_user on table authorization_part;
grant select(key) on table authorization_part to user hive_test_user;
insert overwrite table authorization_part partition (ds='2010') select key, value from src_auth_tmp; 
show grant user hive_test_user on table authorization_part(key) partition (ds='2010');
alter table authorization_part partition (ds='2010') rename to partition (ds='2010_tmp');
show grant user hive_test_user on table authorization_part(key) partition (ds='2010_tmp');

drop table authorization_part;
