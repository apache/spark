set hive.exec.compress.output = true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.minsize=256;
set mapreduce.input.fileinputformat.split.minsize.per.node=256;
set mapreduce.input.fileinputformat.split.minsize.per.rack=256;
set mapreduce.input.fileinputformat.split.maxsize=256;


drop table combine_3_srcpart_seq_rc;

create table combine_3_srcpart_seq_rc (key int , value string) partitioned by (ds string, hr string) stored as sequencefile;

insert overwrite table combine_3_srcpart_seq_rc partition (ds="2010-08-03", hr="00") select * from src;

alter table combine_3_srcpart_seq_rc set fileformat rcfile;
insert overwrite table combine_3_srcpart_seq_rc partition (ds="2010-08-03", hr="001") select * from src;

desc extended combine_3_srcpart_seq_rc partition(ds="2010-08-03", hr="00");
desc extended combine_3_srcpart_seq_rc partition(ds="2010-08-03", hr="001");

select key, value, ds, hr from combine_3_srcpart_seq_rc where ds="2010-08-03" order by key, hr limit 30;

set hive.enforce.bucketing = true;
set hive.exec.reducers.max = 1;

drop table bucket3_1;
CREATE TABLE combine_3_srcpart_seq_rc_bucket(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS stored as sequencefile;

insert overwrite table combine_3_srcpart_seq_rc_bucket partition (ds='1')
select * from src;

alter table combine_3_srcpart_seq_rc_bucket set fileformat rcfile;

insert overwrite table combine_3_srcpart_seq_rc_bucket partition (ds='11')
select * from src;

select key, ds from combine_3_srcpart_seq_rc_bucket tablesample (bucket 1 out of 2) s where ds = '1' or ds= '11' order by key, ds limit 30;

drop table combine_3_srcpart_seq_rc_bucket;

drop table combine_3_srcpart_seq_rc;
