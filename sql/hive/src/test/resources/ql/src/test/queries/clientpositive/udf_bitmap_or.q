set hive.fetch.task.conversion=more;

select ewah_bitmap_or(array(13,2,4,8589934592,4096,0), array(13,2,4,8589934592,4096,0)) from src tablesample (1 rows);
select ewah_bitmap_or(array(13,2,4,8589934592,4096,0), array(8,2,4,8589934592,128,0)) from src tablesample (1 rows);

drop table bitmap_test;
create table bitmap_test (a array<bigint>, b array<bigint>);

insert overwrite table bitmap_test
select array(13,2,4,8589934592,4096,0), array(8,2,4,8589934592,128,0) from src tablesample (10 rows);

select ewah_bitmap_or(a,b) from bitmap_test;

drop table bitmap_test;
