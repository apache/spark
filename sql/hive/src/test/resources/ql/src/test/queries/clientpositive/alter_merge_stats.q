create table src_rc_merge_test_stat(key int, value string) stored as rcfile;

load data local inpath '../../data/files/smbbucket_1.rc' into table src_rc_merge_test_stat;
load data local inpath '../../data/files/smbbucket_2.rc' into table src_rc_merge_test_stat;
load data local inpath '../../data/files/smbbucket_3.rc' into table src_rc_merge_test_stat;

show table extended like `src_rc_merge_test_stat`;
desc extended src_rc_merge_test_stat;

analyze table src_rc_merge_test_stat compute statistics;

desc extended src_rc_merge_test_stat;

alter table src_rc_merge_test_stat concatenate;

show table extended like `src_rc_merge_test_stat`;
desc extended src_rc_merge_test_stat;


create table src_rc_merge_test_part_stat(key int, value string) partitioned by (ds string) stored as rcfile;

alter table src_rc_merge_test_part_stat add partition (ds='2011');

load data local inpath '../../data/files/smbbucket_1.rc' into table src_rc_merge_test_part_stat partition (ds='2011');
load data local inpath '../../data/files/smbbucket_2.rc' into table src_rc_merge_test_part_stat partition (ds='2011');
load data local inpath '../../data/files/smbbucket_3.rc' into table src_rc_merge_test_part_stat partition (ds='2011');

show table extended like `src_rc_merge_test_part_stat` partition (ds='2011');
desc extended src_rc_merge_test_part_stat;

analyze table src_rc_merge_test_part_stat partition(ds='2011') compute statistics;

desc extended src_rc_merge_test_part_stat;

alter table src_rc_merge_test_part_stat partition (ds='2011') concatenate;

show table extended like `src_rc_merge_test_part_stat` partition (ds='2011');
desc extended src_rc_merge_test_part_stat;

drop table src_rc_merge_test_stat;
drop table src_rc_merge_test_part_stat;