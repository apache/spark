create table authorization_fail_2 (key int, value string) partitioned by (ds string);

set hive.security.authorization.enabled=true;

alter table authorization_fail_2 add partition (ds='2010');


