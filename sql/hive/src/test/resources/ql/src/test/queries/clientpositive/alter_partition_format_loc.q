create table alter_partition_format_test (key int, value string);
desc extended alter_partition_format_test;

alter table alter_partition_format_test set fileformat rcfile;
desc extended alter_partition_format_test;

alter table alter_partition_format_test set location "file:/test/test/";
desc extended alter_partition_format_test;

drop table alter_partition_format_test;

--partitioned table
create table alter_partition_format_test (key int, value string) partitioned by (ds string);

alter table alter_partition_format_test add partition(ds='2010');
desc extended alter_partition_format_test partition(ds='2010');

alter table alter_partition_format_test partition(ds='2010') set fileformat rcfile;
desc extended alter_partition_format_test partition(ds='2010');

alter table alter_partition_format_test partition(ds='2010') set location "file:/test/test/ds=2010";
desc extended alter_partition_format_test partition(ds='2010');

desc extended alter_partition_format_test;

alter table alter_partition_format_test set fileformat rcfile;
desc extended alter_partition_format_test;

alter table alter_partition_format_test set location "file:/test/test/";
desc extended alter_partition_format_test;

drop table alter_partition_format_test;