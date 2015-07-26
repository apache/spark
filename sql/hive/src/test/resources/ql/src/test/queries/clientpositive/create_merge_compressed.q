create table src_rc_merge_test(key int, value string) stored as rcfile;

load data local inpath '../../data/files/smbbucket_1.rc' into table src_rc_merge_test;

set hive.exec.compress.output = true;

create table tgt_rc_merge_test(key int, value string) stored as rcfile;
insert into table tgt_rc_merge_test select * from src_rc_merge_test;
insert into table tgt_rc_merge_test select * from src_rc_merge_test;

show table extended like `tgt_rc_merge_test`;

select count(1) from tgt_rc_merge_test;
select sum(hash(key)), sum(hash(value)) from tgt_rc_merge_test;

alter table tgt_rc_merge_test concatenate;

show table extended like `tgt_rc_merge_test`;

select count(1) from tgt_rc_merge_test;
select sum(hash(key)), sum(hash(value)) from tgt_rc_merge_test;

drop table src_rc_merge_test;
drop table tgt_rc_merge_test;