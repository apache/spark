drop table tstsrc;
drop table tstsrcpart;

create table tstsrc like src;
insert overwrite table tstsrc select key, value from src;

create table tstsrcpart like srcpart;
insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='12')
select key, value from srcpart where ds='2008-04-08' and hr='12';


ALTER TABLE tstsrc TOUCH;
ALTER TABLE tstsrcpart TOUCH;
ALTER TABLE tstsrcpart TOUCH PARTITION (ds='2008-04-08', hr='12');

drop table tstsrc;
drop table tstsrcpart;
