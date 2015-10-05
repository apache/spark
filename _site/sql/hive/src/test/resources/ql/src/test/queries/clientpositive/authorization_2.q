-- SORT_BEFORE_DIFF

create table authorization_part (key int, value string) partitioned by (ds string);
create table src_auth_tmp as select * from src;
ALTER TABLE authorization_part SET TBLPROPERTIES ("PARTITION_LEVEL_PRIVILEGE"="TRUE");
set hive.security.authorization.enabled=true;

-- column grant to user
grant Create on  authorization_part to user hive_test_user;
grant Update on table authorization_part to user hive_test_user;
grant Drop on table authorization_part to user hive_test_user;
grant select on table src_auth_tmp to user hive_test_user;

show grant user hive_test_user on table authorization_part;

alter table authorization_part add partition (ds='2010');
show grant user hive_test_user on table authorization_part partition (ds='2010');

grant select(key) on table authorization_part to user hive_test_user;
alter table authorization_part drop partition (ds='2010');
insert overwrite table authorization_part partition (ds='2010') select key, value from src_auth_tmp; 
show grant user hive_test_user on table authorization_part(key) partition (ds='2010');
show grant user hive_test_user on table authorization_part(key);
select key from authorization_part where ds='2010' order by key limit 20;

revoke select(key) on table authorization_part from user hive_test_user;
show grant user hive_test_user on table authorization_part(key);
show grant user hive_test_user on table authorization_part(key) partition (ds='2010');

select key from authorization_part where ds='2010' order by key limit 20;

revoke select(key) on table authorization_part partition (ds='2010') from user hive_test_user;
show grant user hive_test_user on table authorization_part(key) partition (ds='2010');

alter table authorization_part drop partition (ds='2010');

-- table grant to user
show grant user hive_test_user on table authorization_part;

alter table authorization_part add partition (ds='2010');
show grant user hive_test_user on table authorization_part partition (ds='2010');

grant select on table authorization_part to user hive_test_user;
alter table authorization_part drop partition (ds='2010');
insert overwrite table authorization_part partition (ds='2010') select key, value from src_auth_tmp; 
show grant user hive_test_user on table authorization_part partition (ds='2010');
show grant user hive_test_user on table authorization_part;
select key from authorization_part where ds='2010' order by key limit 20;

revoke select on table authorization_part from user hive_test_user;
show grant user hive_test_user on table authorization_part;
show grant user hive_test_user on table authorization_part partition (ds='2010');

select key from authorization_part where ds='2010' order by key limit 20;

revoke select on table authorization_part partition (ds='2010') from user hive_test_user;
show grant user hive_test_user on table authorization_part partition (ds='2010');

alter table authorization_part drop partition (ds='2010');

-- column grant to group

show grant group hive_test_group1 on table authorization_part;

alter table authorization_part add partition (ds='2010');
show grant group hive_test_group1 on table authorization_part partition (ds='2010');

grant select(key) on table authorization_part to group hive_test_group1;
alter table authorization_part drop partition (ds='2010');
insert overwrite table authorization_part partition (ds='2010') select key, value from src_auth_tmp; 
show grant group hive_test_group1 on table authorization_part(key) partition (ds='2010');
show grant group hive_test_group1 on table authorization_part(key);
select key from authorization_part where ds='2010' order by key limit 20;

revoke select(key) on table authorization_part from group hive_test_group1;
show grant group hive_test_group1 on table authorization_part(key);
show grant group hive_test_group1 on table authorization_part(key) partition (ds='2010');

select key from authorization_part where ds='2010' order by key limit 20;

revoke select(key) on table authorization_part partition (ds='2010') from group hive_test_group1;
show grant group hive_test_group1 on table authorization_part(key) partition (ds='2010');

alter table authorization_part drop partition (ds='2010');

-- table grant to group
show grant group hive_test_group1 on table authorization_part;

alter table authorization_part add partition (ds='2010');
show grant group hive_test_group1 on table authorization_part partition (ds='2010');

grant select on table authorization_part to group hive_test_group1;
alter table authorization_part drop partition (ds='2010');
insert overwrite table authorization_part partition (ds='2010') select key, value from src_auth_tmp; 
show grant group hive_test_group1 on table authorization_part partition (ds='2010');
show grant group hive_test_group1 on table authorization_part;
select key from authorization_part where ds='2010' order by key limit 20;

revoke select on table authorization_part from group hive_test_group1;
show grant group hive_test_group1 on table authorization_part;
show grant group hive_test_group1 on table authorization_part partition (ds='2010');

select key from authorization_part where ds='2010' order by key limit 20;

revoke select on table authorization_part partition (ds='2010') from group hive_test_group1;
show grant group hive_test_group1 on table authorization_part partition (ds='2010');


revoke select on table src_auth_tmp from user hive_test_user;
set hive.security.authorization.enabled=false;
drop table authorization_part;