create table src_10 as select * from src limit 10;

create table src_lv1 (key string, value string);
create table src_lv2 (key string, value string);
create table src_lv3 (key string, value string);

-- 2LV
-- TS[0]-LVF[1]-SEL[2]-LVJ[5]-SEL[11]-FS[12]
--             -SEL[3]-UDTF[4]-LVJ[5]
--      -LVF[6]-SEL[7]-LVJ[10]-SEL[13]-FS[14]
--             -SEL[8]-UDTF[9]-LVJ[10]
explain
from src_10
insert overwrite table src_lv1 select key, C lateral view explode(array(key+1, key+2)) A as C
insert overwrite table src_lv2 select key, C lateral view explode(array(key+3, key+4)) A as C;

from src_10
insert overwrite table src_lv1 select key, C lateral view explode(array(key+1, key+2)) A as C
insert overwrite table src_lv2 select key, C lateral view explode(array(key+3, key+4)) A as C;

select * from src_lv1 order by key, value;
select * from src_lv2 order by key, value;

-- 2(LV+GBY)
-- TS[0]-LVF[1]-SEL[2]-LVJ[5]-SEL[11]-GBY[12]-RS[13]-GBY[14]-SEL[15]-FS[16]
--             -SEL[3]-UDTF[4]-LVJ[5]
--      -LVF[6]-SEL[7]-LVJ[10]-SEL[17]-GBY[18]-RS[19]-GBY[20]-SEL[21]-FS[22]
--             -SEL[8]-UDTF[9]-LVJ[10]
explain
from src_10
insert overwrite table src_lv1 select key, sum(C) lateral view explode(array(key+1, key+2)) A as C group by key
insert overwrite table src_lv2 select key, sum(C) lateral view explode(array(key+3, key+4)) A as C group by key;

from src_10
insert overwrite table src_lv1 select key, sum(C) lateral view explode(array(key+1, key+2)) A as C group by key
insert overwrite table src_lv2 select key, sum(C) lateral view explode(array(key+3, key+4)) A as C group by key;

select * from src_lv1 order by key, value;
select * from src_lv2 order by key, value;

-- (LV+GBY) + RS:2GBY
-- TS[0]-LVF[1]-SEL[2]-LVJ[5]-SEL[6]-GBY[7]-RS[8]-GBY[9]-SEL[10]-FS[11]
--             -SEL[3]-UDTF[4]-LVJ[5]
--      -FIL[12]-SEL[13]-RS[14]-FOR[15]-FIL[16]-GBY[17]-SEL[18]-FS[19]
--                                     -FIL[20]-GBY[21]-SEL[22]-FS[23]
explain
from src_10
insert overwrite table src_lv1 select key, sum(C) lateral view explode(array(key+1, key+2)) A as C group by key
insert overwrite table src_lv2 select key, count(value) where key > 200 group by key
insert overwrite table src_lv3 select key, count(value) where key < 200 group by key;

from src_10
insert overwrite table src_lv1 select key, sum(C) lateral view explode(array(key+1, key+2)) A as C group by key
insert overwrite table src_lv2 select key, count(value) where key > 200 group by key
insert overwrite table src_lv3 select key, count(value) where key < 200 group by key;

select * from src_lv1 order by key, value;
select * from src_lv2 order by key, value;
select * from src_lv3 order by key, value;

-- todo: shared distinct columns (should work with hive.optimize.multigroupby.common.distincts)
-- 2(LV+GBY) + RS:2GBY
-- TS[0]-LVF[1]-SEL[2]-LVJ[5]-SEL[11]-GBY[12]-RS[13]-GBY[14]-SEL[15]-FS[16]
--             -SEL[3]-UDTF[4]-LVJ[5]
--      -LVF[6]-SEL[7]-LVJ[10]-SEL[17]-GBY[18]-RS[19]-GBY[20]-SEL[21]-FS[22]
--             -SEL[8]-UDTF[9]-LVJ[10]
--      -SEL[23]-GBY[24]-RS[25]-GBY[26]-SEL[27]-FS[28]
explain
from src_10
insert overwrite table src_lv1 select C, sum(distinct key) lateral view explode(array(key+1, key+2)) A as C group by C
insert overwrite table src_lv2 select C, sum(distinct key) lateral view explode(array(key+3, key+4)) A as C group by C
insert overwrite table src_lv3 select value, sum(distinct key) group by value;

from src_10
insert overwrite table src_lv1 select C, sum(distinct key) lateral view explode(array(key+1, key+2)) A as C group by C
insert overwrite table src_lv2 select C, sum(distinct key) lateral view explode(array(key+3, key+4)) A as C group by C
insert overwrite table src_lv3 select value, sum(distinct key) group by value;

select * from src_lv1 order by key, value;
select * from src_lv2 order by key, value;
select * from src_lv3 order by key, value;

create table src_lv4 (key string, value string);

-- Common distincts optimization works across non-lateral view queries, but not across lateral view multi inserts
explain
from src_10
insert overwrite table src_lv1 select key, sum(distinct C) lateral view explode(array(key+1, key+2)) A as C group by key
insert overwrite table src_lv2 select key, sum(distinct C) lateral view explode(array(key+3, key+4)) A as C group by key
insert overwrite table src_lv3 select value, sum(distinct key) where key > 200 group by value
insert overwrite table src_lv4 select value, sum(distinct key) where key < 200 group by value;

from src_10
insert overwrite table src_lv1 select key, sum(distinct C) lateral view explode(array(key+1, key+2)) A as C group by key
insert overwrite table src_lv2 select key, sum(distinct C) lateral view explode(array(key+3, key+4)) A as C group by key
insert overwrite table src_lv3 select value, sum(distinct key) where key > 200 group by value
insert overwrite table src_lv4 select value, sum(distinct key) where key < 200 group by value;

select * from src_lv1 order by key, value;
select * from src_lv2 order by key, value;
select * from src_lv3 order by key, value;
select * from src_lv4 order by key, value;
