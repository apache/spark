

create table fileformat_mix_test (src int, value string) partitioned by (ds string);
alter table fileformat_mix_test set fileformat Sequencefile;

insert overwrite table fileformat_mix_test partition (ds='1')
select key, value from src;

alter table fileformat_mix_test add partition (ds='2');

alter table fileformat_mix_test set fileformat rcfile;

select count(1) from fileformat_mix_test;

select src from fileformat_mix_test;

