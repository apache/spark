-- SORT_BEFORE_DIFF

create table authorization_fail_4 (key int, value string) partitioned by (ds string);

set hive.security.authorization.enabled=true;
grant Alter on table authorization_fail_4 to user hive_test_user;
ALTER TABLE authorization_fail_4 SET TBLPROPERTIES ("PARTITION_LEVEL_PRIVILEGE"="TRUE");

grant Create on table authorization_fail_4 to user hive_test_user;
alter table authorization_fail_4 add partition (ds='2010');

show grant user hive_test_user on table authorization_fail_4;
show grant user hive_test_user on table authorization_fail_4 partition (ds='2010');

select key from authorization_fail_4 where ds='2010';
