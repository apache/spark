drop table tst_src1;
create table tst_src1 like src1;
load data local inpath '../data/files/kv1.txt' into table tst_src1 ;
select count(1) from tst_src1;
load data local inpath '../data/files/kv1.txt' into table tst_src1 ;
select count(1) from tst_src1;
drop table tst_src1;
