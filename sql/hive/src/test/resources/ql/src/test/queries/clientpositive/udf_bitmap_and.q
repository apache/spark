select ewah_bitmap_and(array(13,2,4,8589934592,4096,0), array(13,2,4,8589934592,4096,0)) from src limit 1;
select ewah_bitmap_and(array(13,2,4,8589934592,4096,0), array(8,2,4,8589934592,128,0)) from src limit 1;

drop table bitmap_test;
create table bitmap_test (a array<bigint>, b array<bigint>);

insert overwrite table bitmap_test
select array(13,2,4,8589934592,4096,0), array(8,2,4,8589934592,128,0) from src limit 10;

select ewah_bitmap_and(a,b) from bitmap_test;

drop table bitmap_test;
