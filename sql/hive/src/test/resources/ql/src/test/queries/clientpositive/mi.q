set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
create table nzhang_t1 like srcpart;
create table nzhang_t2 like srcpart;

FROM srcpart 
INSERT OVERWRITE TABLE nzhang_t1 PARTITION (ds, hr) 
SELECT key, value, ds, hr
WHERE ds = '2008-04-08' AND hr = '11'
INSERT OVERWRITE TABLE nzhang_t2 PARTITION (ds, hr) 
SELECT key, value, ds, hr
WHERE ds = '2008-04-08' and hr = '12'
GROUP BY key, value, ds, hr;

show partitions nzhang_t1;
show partitions nzhang_t2;

select * from nzhang_t1 order by key, value;
select * from nzhang_t2 order by key, value;


