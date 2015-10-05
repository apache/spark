set hive.mapred.mode=strict;

create table dest_sp (cnt int);

insert overwrite table dest_sp
select * from 
  (select count(1) as cnt from src 
    union all
   select count(1) as cnt from srcpart where ds = '2009-08-09'
  )x;

select * from dest_sp x order by x.cnt limit 2;


