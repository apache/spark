-- SORT_BEFORE_DIFF

create table test_user (user string, `group` string);
grant select on table test_user to user hive_test;

explain select user from test_user;

show grant user hive_test on table test_user;

drop table test_user;

create table test_user (role string, `group` string);
grant select on table test_user to user hive_test;

explain select role from test_user;

show grant user hive_test on table test_user;

drop table test_user;