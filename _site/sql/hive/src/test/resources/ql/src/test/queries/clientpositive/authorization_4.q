-- SORT_BEFORE_DIFF

create table src_autho_test as select * from src;

grant All on table src_autho_test to user hive_test_user;

set hive.security.authorization.enabled=true;

show grant user hive_test_user on table src_autho_test;

select key from src_autho_test order by key limit 20;

drop table src_autho_test;