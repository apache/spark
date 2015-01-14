create database lockneg1;
use lockneg1;

create table tstsrcpart like default.srcpart;

insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='11')
select key, value from default.srcpart where ds='2008-04-08' and hr='11';

lock database lockneg1 shared;
show locks database lockneg1;
select count(1) from tstsrcpart where ds='2008-04-08' and hr='11';

unlock database lockneg1;
show locks database lockneg1;
lock database lockneg1 exclusive;
show locks database lockneg1;
select count(1) from tstsrcpart where ds='2008-04-08' and hr='11';
