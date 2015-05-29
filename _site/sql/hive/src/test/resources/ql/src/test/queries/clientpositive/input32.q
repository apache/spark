


set hive.test.mode=true;
set hive.test.mode.prefix=tst_;
set hive.test.mode.nosamplelist=src,srcbucket;

create table dest32(a int);
create table tst_dest32(a int);

explain 
insert overwrite table dest32
select count(1) from srcbucket;       

insert overwrite table dest32
select count(1) from srcbucket;       

set hive.test.mode=false;

select * from tst_dest32;





