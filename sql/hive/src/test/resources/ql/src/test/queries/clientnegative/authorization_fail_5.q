-- SORT_BEFORE_DIFF

create table authorization_fail (key int, value string) partitioned by (ds string);
set hive.security.authorization.enabled=true;

grant Alter on table authorization_fail to user hive_test_user;
ALTER TABLE authorization_fail SET TBLPROPERTIES ("PARTITION_LEVEL_PRIVILEGE"="TRUE");

grant Create on table authorization_fail to user hive_test_user;
grant Select on table authorization_fail to user hive_test_user;
alter table authorization_fail add partition (ds='2010');

show grant user hive_test_user on table authorization_fail;
show grant user hive_test_user on table authorization_fail partition (ds='2010');

revoke Select on table authorization_fail partition (ds='2010') from user hive_test_user;

show grant user hive_test_user on table authorization_fail partition (ds='2010');

select key from authorization_fail where ds='2010';