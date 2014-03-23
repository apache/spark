
create table addpart1 (a int) partitioned by (b string, c string);

alter table addpart1 add partition (b='f', c='s');

show partitions addpart1;

alter table addpart1 add partition (b='f', c='');

show prtitions addpart1;

