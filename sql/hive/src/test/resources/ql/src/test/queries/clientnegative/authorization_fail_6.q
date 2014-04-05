-- SORT_BEFORE_DIFF

create table authorization_part_fail (key int, value string) partitioned by (ds string);
set hive.security.authorization.enabled=true;

ALTER TABLE authorization_part_fail SET TBLPROPERTIES ("PARTITION_LEVEL_PRIVILEGE"="TRUE");
