create table if not exists authorization_invalid_v1 (key int, value string);
grant delete on table authorization_invalid_v1 to user hive_test_user;
drop table authorization_invalid_v1;



