drop table tstsrc;

set hive.exec.pre.hooks = org.apache.hadoop.hive.ql.hooks.PreExecutePrinter,org.apache.hadoop.hive.ql.hooks.EnforceReadOnlyTables,org.apache.hadoop.hive.ql.hooks.UpdateInputAccessTimeHook$PreExec;

create table tstsrc as select * from src;
desc extended tstsrc;
select count(1) from tstsrc;
desc extended tstsrc;
drop table tstsrc;

drop table tstsrcpart;
create table tstsrcpart like srcpart;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;


insert overwrite table tstsrcpart partition (ds, hr) select key, value, ds, hr from srcpart;

desc extended tstsrcpart;
desc extended tstsrcpart partition (ds='2008-04-08', hr='11');
desc extended tstsrcpart partition (ds='2008-04-08', hr='12');

select count(1) from tstsrcpart where ds = '2008-04-08' and hr = '11';

desc extended tstsrcpart;
desc extended tstsrcpart partition (ds='2008-04-08', hr='11');
desc extended tstsrcpart partition (ds='2008-04-08', hr='12');

drop table tstsrcpart;
