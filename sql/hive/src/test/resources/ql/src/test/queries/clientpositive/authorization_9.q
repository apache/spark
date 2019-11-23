-- SORT_BEFORE_DIFF

create table dummy (key string, value string);

grant select on database default to user hive_test_user;
grant select on table dummy to user hive_test_user;
grant select (key, value) on table dummy to user hive_test_user;

show grant user hive_test_user on database default;
show grant user hive_test_user on table dummy;
show grant user hive_test_user on all;

grant select on database default to user hive_test_user2;
grant select on table dummy to user hive_test_user2;
grant select (key, value) on table dummy to user hive_test_user2;

show grant on all;
