

create table join_rc1(key string, value string) stored as RCFile;
create table join_rc2(key string, value string) stored as RCFile;
insert overwrite table join_rc1 select * from src;
insert overwrite table join_rc2 select * from src;

explain
select join_rc1.key, join_rc2.value
FROM join_rc1 JOIN join_rc2 ON join_rc1.key = join_rc2.key;

select join_rc1.key, join_rc2.value
FROM join_rc1 JOIN join_rc2 ON join_rc1.key = join_rc2.key;



