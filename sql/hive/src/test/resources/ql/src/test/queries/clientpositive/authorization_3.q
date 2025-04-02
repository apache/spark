-- SORT_BEFORE_DIFF

create table src_autho_test as select * from src;

grant drop on table src_autho_test to user hive_test_user;
grant select on table src_autho_test to user hive_test_user;

show grant user hive_test_user on table src_autho_test;

revoke select on table src_autho_test from user hive_test_user;
revoke drop on table src_autho_test from user hive_test_user;

grant drop,select on table src_autho_test to user hive_test_user;
show grant user hive_test_user on table src_autho_test;
revoke drop,select on table src_autho_test from user hive_test_user;

grant drop,select(key), select(value) on table src_autho_test to user hive_test_user;
show grant user hive_test_user on table src_autho_test;
revoke drop,select(key), select(value) on table src_autho_test from user hive_test_user;
