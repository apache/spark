create table ppr_test (key string) partitioned by (ds string);

alter table ppr_test add partition (ds = '1234');
alter table ppr_test add partition (ds = '1224');
alter table ppr_test add partition (ds = '1214');
alter table ppr_test add partition (ds = '12+4');
alter table ppr_test add partition (ds = '12.4');
alter table ppr_test add partition (ds = '12:4');
alter table ppr_test add partition (ds = '12%4');
alter table ppr_test add partition (ds = '12*4');

insert overwrite table ppr_test partition(ds = '1234') select * from (select '1234' from src limit 1 union all select 'abcd' from src limit 1) s;
insert overwrite table ppr_test partition(ds = '1224') select * from (select '1224' from src limit 1 union all select 'abcd' from src limit 1) s;
insert overwrite table ppr_test partition(ds = '1214') select * from (select '1214' from src limit 1 union all select 'abcd' from src limit 1) s;
insert overwrite table ppr_test partition(ds = '12+4') select * from (select '12+4' from src limit 1 union all select 'abcd' from src limit 1) s;
insert overwrite table ppr_test partition(ds = '12.4') select * from (select '12.4' from src limit 1 union all select 'abcd' from src limit 1) s;
insert overwrite table ppr_test partition(ds = '12:4') select * from (select '12:4' from src limit 1 union all select 'abcd' from src limit 1) s;
insert overwrite table ppr_test partition(ds = '12%4') select * from (select '12%4' from src limit 1 union all select 'abcd' from src limit 1) s;
insert overwrite table ppr_test partition(ds = '12*4') select * from (select '12*4' from src limit 1 union all select 'abcd' from src limit 1) s;


select * from ppr_test where ds = '1234' order by key;
select * from ppr_test where ds = '1224' order by key;
select * from ppr_test where ds = '1214' order by key;
select * from ppr_test where ds = '12.4' order by key;
select * from ppr_test where ds = '12+4' order by key;
select * from ppr_test where ds = '12:4' order by key;
select * from ppr_test where ds = '12%4' order by key;
select * from ppr_test where ds = '12*4' order by key;
select * from ppr_test where ds = '12.*4' order by key;

select * from ppr_test where ds = '1234' and key = '1234';
select * from ppr_test where ds = '1224' and key = '1224';
select * from ppr_test where ds = '1214' and key = '1214';
select * from ppr_test where ds = '12.4' and key = '12.4';
select * from ppr_test where ds = '12+4' and key = '12+4';
select * from ppr_test where ds = '12:4' and key = '12:4';
select * from ppr_test where ds = '12%4' and key = '12%4';
select * from ppr_test where ds = '12*4' and key = '12*4';


