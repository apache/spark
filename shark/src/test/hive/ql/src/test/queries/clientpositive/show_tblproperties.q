
create table tmpfoo (a String);
show tblproperties tmpfoo("bar");

alter table tmpfoo set tblproperties ("bar" = "bar value");
alter table tmpfoo set tblproperties ("tmp" = "true");

show tblproperties tmpfoo;
show tblproperties tmpfoo("bar");

drop table tmpfoo;
