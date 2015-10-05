-- SORT_BEFORE_DIFF

create table authorization_fail (key int, value string);

set hive.security.authorization.enabled=true;

create role hive_test_role_fail;

grant role hive_test_role_fail to user hive_test_user;
grant select on table authorization_fail to role hive_test_role_fail;
show role grant user hive_test_user;

show grant role hive_test_role_fail on table authorization_fail;

drop role hive_test_role_fail;

select key from authorization_fail;