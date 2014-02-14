set hive.exec.concatenate.check.index =false;
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

show table extended like `src_rc_concatenate_test`;

select count(1) from src_rc_concatenate_test;
select sum(hash(key)), sum(hash(value)) from src_rc_concatenate_test;

drop index src_rc_concatenate_test_index on src_rc_concatenate_test;

create table src_rc_concatenate_test_part(key int, value string) partitioned by (ds string) stored as rcfile;

alter table src_rc_concatenate_test_part add partition (ds='2011');

load data local inpath '../data/files/smbbucket_1.rc' into table src_rc_concatenate_test_part partition (ds='2011');
load data local inpath '../data/files/smbbucket_2.rc' into table src_rc_concatenate_test_part partition (ds='2011');
load data local inpath '../data/files/smbbucket_3.rc' into table src_rc_concatenate_test_part partition (ds='2011');

show table extended like `src_rc_concatenate_test_part` partition (ds='2011');

select count(1) from src_rc_concatenate_test_part;
select sum(hash(key)), sum(hash(value)) from src_rc_concatenate_test_part;

create index src_rc_concatenate_test_part_index on table src_rc_concatenate_test_part(key) as 'compact' WITH DEFERRED REBUILD IDXPROPERTIES ("prop1"="val1", "prop2"="val2");
show indexes on src_rc_concatenate_test_part;

alter table src_rc_concatenate_test_part partition (ds='2011') concatenate;

show table extended like `src_rc_concatenate_test_part` partition (ds='2011');

select count(1) from src_rc_concatenate_test_part;
select sum(hash(key)), sum(hash(value)) from src_rc_concatenate_test_part;

drop index src_rc_concatenate_test_part_index on src_rc_concatenate_test_part;
