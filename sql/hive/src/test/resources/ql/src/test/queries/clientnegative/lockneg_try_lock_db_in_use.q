set hive.lock.numretries=0;

create database lockneg2;
use lockneg2;

create table tstsrcpart like default.srcpart;

insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='11')
select key, value from default.srcpart where ds='2008-04-08' and hr='11';

lock table tstsrcpart shared;
show locks;

lock database lockneg2 exclusive;
show locks;
