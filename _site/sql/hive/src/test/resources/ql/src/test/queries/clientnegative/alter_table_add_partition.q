create table mp (a int) partitioned by (b int);

-- should fail
alter table mp add partition (b='1', c='1');

