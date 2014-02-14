create table authorization_fail_1 (key int, value string);
set hive.security.authorization.enabled=true;

grant Create on table authorization_fail_1 to user hive_test_user;
grant Create on table authorization_fail_1 to user hive_test_user;


