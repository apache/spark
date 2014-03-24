create table ptestfilter (a string, b int) partitioned by (c string, d int);
describe ptestfilter;

alter table ptestfilter add partition (c='US', d=1);
alter table ptestfilter add partition (c='US', d=2);
show partitions ptestfilter;

alter table ptestfilter drop partition (c='US', d<'2');



