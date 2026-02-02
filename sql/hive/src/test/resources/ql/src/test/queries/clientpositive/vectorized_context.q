create table store(s_store_sk int, s_city string)
stored as orc;
insert overwrite table store
select cint, cstring1
from alltypesorc
where cint not in (
-3728, -563, 762, 6981, 253665376, 528534767, 626923679);
create table store_sales(ss_store_sk int, ss_hdemo_sk int, ss_net_profit double)
stored as orc;
insert overwrite table store_sales
select cint, cint, cdouble
from alltypesorc
where cint not in (
-3728, -563, 762, 6981, 253665376, 528534767, 626923679);
create table household_demographics(hd_demo_sk int)
stored as orc;
insert overwrite table household_demographics
select cint
from alltypesorc
where cint not in (
-3728, -563, 762, 6981, 253665376, 528534767, 626923679);
set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=true;


explain 
select store.s_city, ss_net_profit
from store_sales
JOIN store ON store_sales.ss_store_sk = store.s_store_sk
JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
limit 100
;

select store.s_city, ss_net_profit
from store_sales
JOIN store ON store_sales.ss_store_sk = store.s_store_sk
JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
limit 100
;

set hive.auto.convert.join=false;
set hive.vectorized.execution.enabled=false;

drop table store;
drop table store_sales;
drop table household_demographics;

