-- SORT_BEFORE_DIFF

create table authorization_part_fail (key int, value string) partitioned by (ds string);
ALTER TABLE authorization_part_fail SET TBLPROPERTIES ("PARTITION_LEVEL_PRIVILEGE"="TRUE");
create table src_auth as select * from src;
set hive.security.authorization.enabled=true;

grant Create on table authorization_part_fail to user hive_test_user;
grant Update on table authorization_part_fail to user hive_test_user;
grant Drop on table authorization_part_fail to user hive_test_user;
grant select on table src_auth to user hive_test_user;

-- column grant to group

grant select(key) on table authorization_part_fail to group hive_test_group1;
grant select on table authorization_part_fail to group hive_test_group1;

show grant group hive_test_group1 on table authorization_part_fail;

insert overwrite table authorization_part_fail partition (ds='2010') select key, value from src_auth; 
show grant group hive_test_group1 on table authorization_part_fail(key) partition (ds='2010');
show grant group hive_test_group1 on table authorization_part_fail partition (ds='2010');
select key, value from authorization_part_fail where ds='2010' order by key limit 20;

insert overwrite table authorization_part_fail partition (ds='2011') select key, value from src_auth; 
show grant group hive_test_group1 on table authorization_part_fail(key) partition (ds='2011');
show grant group hive_test_group1 on table authorization_part_fail partition (ds='2011');
select key, value from authorization_part_fail where ds='2011' order by key limit 20;

select key,value, ds from authorization_part_fail where ds>='2010' order by key, ds limit 20;

revoke select on table authorization_part_fail partition (ds='2010') from group hive_test_group1;

select key,value, ds from authorization_part_fail where ds>='2010' order by key, ds limit 20;

drop table authorization_part_fail;
drop table src_auth;