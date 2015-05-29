create table ptestfilter (a string, b int) partitioned by (c int, d int);
describe ptestfilter;

alter table ptestfilter add partition (c=1, d=1);
alter table ptestfilter add partition (c=1, d=2);
alter table ptestFilter add partition (c=2, d=1);
alter table ptestfilter add partition (c=2, d=2);
alter table ptestfilter add partition (c=3, d=1);
alter table ptestfilter add partition (c=30, d=2);
show partitions ptestfilter;

alter table ptestfilter drop partition (c=1, d=1);
show partitions ptestfilter;

alter table ptestfilter drop partition (c=2);
show partitions ptestfilter;

alter table ptestfilter drop partition (c<4);
show partitions ptestfilter;

drop table ptestfilter;


