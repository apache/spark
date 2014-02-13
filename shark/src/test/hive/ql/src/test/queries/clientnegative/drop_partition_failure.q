create table mp (a string) partitioned by (b string, c string);

alter table mp add partition (b='1', c='1');
alter table mp add partition (b='1', c='2');
alter table mp add partition (b='2', c='2');

show partitions mp;

set hive.exec.drop.ignorenonexistent=false;
-- Can't use DROP PARTITION if the partition doesn't exist and IF EXISTS isn't specified
alter table mp drop partition (b='3');
