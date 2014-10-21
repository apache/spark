-- SORT_BEFORE_DIFF

create table authorization_fail_3 (key int, value string) partitioned by (ds string);
set hive.security.authorization.enabled=true;

grant Create on table authorization_fail_3 to user hive_test_user;
alter table authorization_fail_3 add partition (ds='2010');

show grant user hive_test_user on table authorization_fail_3;
show grant user hive_test_user on table authorization_fail_3 partition (ds='2010');

select key from authorization_fail_3 where ds='2010';
