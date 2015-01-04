set hive.security.authorization.enabled=false;
create database db_fail_to_drop;
set hive.security.authorization.enabled=true;

drop database db_fail_to_drop;
