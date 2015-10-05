create table ptestfilter (a string, b int) partitioned by (c string, d string);
describe ptestfilter;

alter table ptestfilter add partition (c='US', d=1);
alter table ptestfilter add partition (c='US', d=2);
alter table ptestFilter add partition (c='Uganda', d=2);
alter table ptestfilter add partition (c='Germany', d=2);
alter table ptestfilter add partition (c='Canada', d=3);
alter table ptestfilter add partition (c='Russia', d=3);
alter table ptestfilter add partition (c='Greece', d=2);
alter table ptestfilter add partition (c='India', d=3);
alter table ptestfilter add partition (c='France', d=4);
show partitions ptestfilter;

alter table ptestfilter drop partition (c='US', d<'2');
show partitions ptestfilter;

alter table ptestfilter drop partition (c>='US', d<='2');
show partitions ptestfilter;

alter table ptestfilter drop partition (c >'India');
show partitions ptestfilter;

alter table ptestfilter drop partition (c >='India'),
                             partition (c='Greece', d='2');
show partitions ptestfilter;

alter table ptestfilter drop partition (c != 'France');
show partitions ptestfilter;

set hive.exec.drop.ignorenonexistent=false;
alter table ptestfilter drop if exists partition (c='US');
show partitions ptestfilter;

drop table ptestfilter;


