set hive.exec.concatenate.check.index=true;
create table src_rc_concatenate_test(key int, value string) stored as rcfile;

load data local inpath '../data/files/smbbucket_1.rc' into table src_rc_concatenate_test;
load data local inpath '../data/files/smbbucket_2.rc' into table src_rc_concatenate_test;
load data local inpath '../data/files/smbbucket_3.rc' into table src_rc_concatenate_test;

show table extended like `src_rc_concatenate_test`;

select count(1) from src_rc_concatenate_test;
select sum(hash(key)), sum(hash(value)) from src_rc_concatenate_test;

create index src_rc_concatenate_test_index on table src_rc_concatenate_test(key) as 'compact' WITH DEFERRED REBUILD IDXPROPERTIES ("prop1"="val1", "prop2"="val2"); 
show indexes on src_rc_concatenate_test;

alter table src_rc_concatenate_test concatenate;
