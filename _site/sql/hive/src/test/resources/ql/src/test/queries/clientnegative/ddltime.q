
create table T2 like srcpart;

insert overwrite table T2 partition (ds = '2010-06-21', hr='1') select /*+ HOLD_DDLTIME */ key, value from src where key > 10;


