create table T1 like src;

desc extended T1;

!sleep 1;
insert overwrite table T1 select * from src;

desc extended T1;

!sleep 1;

insert overwrite table T1 select /*+ HOLD_DDLTIME*/ * from src;

desc extended T1;

!sleep 1;

insert overwrite table T1 select * from src;

desc extended T1;



create table if not exists T2 like srcpart;
desc extended T2;

!sleep 1;

insert overwrite table T2 partition (ds = '2010-06-21', hr = '1') select key, value from src where key > 10;

desc extended T2 partition (ds = '2010-06-21', hr = '1');

!sleep 1;

insert overwrite table T2 partition (ds = '2010-06-21', hr='1') select /*+ HOLD_DDLTIME */ key, value from src where key > 10;

desc extended T2 partition (ds = '2010-06-21', hr = '1');

!sleep 1;

insert overwrite table T2 partition (ds='2010-06-01', hr='1') select key, value from src where key > 10;

desc extended T2 partition(ds='2010-06-01', hr='1');


