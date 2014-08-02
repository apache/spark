create table mp (a string) partitioned by (b string, c string);

alter table mp add partition (b='1', c='1');
alter table mp add partition (b='1', c='2');
alter table mp add partition (b='2', c='2');

show partitions mp;

explain extended alter table mp drop partition (b='1');
alter table mp drop partition (b='1');

show partitions mp;

set hive.exec.drop.ignorenonexistent=false;
alter table mp drop if exists partition (b='3');

show partitions mp;
