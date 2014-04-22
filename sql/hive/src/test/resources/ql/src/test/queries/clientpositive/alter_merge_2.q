create table src_rc_merge_test_part(key int, value string) partitioned by (ds string, ts string) stored as rcfile;

alter table src_rc_merge_test_part add partition (ds='2012-01-03', ts='2012-01-03+14:46:31');
desc extended src_rc_merge_test_part partition (ds='2012-01-03', ts='2012-01-03+14:46:31');

load data local inpath '../data/files/smbbucket_1.rc' into table src_rc_merge_test_part partition (ds='2012-01-03', ts='2012-01-03+14:46:31');
load data local inpath '../data/files/smbbucket_2.rc' into table src_rc_merge_test_part partition (ds='2012-01-03', ts='2012-01-03+14:46:31');
load data local inpath '../data/files/smbbucket_3.rc' into table src_rc_merge_test_part partition (ds='2012-01-03', ts='2012-01-03+14:46:31');

select count(1) from src_rc_merge_test_part where ds='2012-01-03' and ts='2012-01-03+14:46:31';
select sum(hash(key)), sum(hash(value)) from src_rc_merge_test_part where ds='2012-01-03' and ts='2012-01-03+14:46:31';

alter table src_rc_merge_test_part partition (ds='2012-01-03', ts='2012-01-03+14:46:31') concatenate;


select count(1) from src_rc_merge_test_part where ds='2012-01-03' and ts='2012-01-03+14:46:31';
select sum(hash(key)), sum(hash(value)) from src_rc_merge_test_part where ds='2012-01-03' and ts='2012-01-03+14:46:31';

drop table src_rc_merge_test_part;
