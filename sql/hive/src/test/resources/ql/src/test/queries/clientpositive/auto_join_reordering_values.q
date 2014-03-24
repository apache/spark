-- HIVE-5056 RS has expression list for values, but it's ignored in MapJoinProcessor

create table testsrc ( `key` int,`val` string);
load data local inpath '../data/files/kv1.txt' overwrite into table testsrc;
drop table if exists orderpayment_small;
create table orderpayment_small (`dealid` int,`date` string,`time` string, `cityid` int, `userid` int);
insert overwrite table orderpayment_small select 748, '2011-03-24', '2011-03-24', 55 ,5372613 from testsrc limit 1;
drop table if exists user_small;
create table user_small( userid int);
insert overwrite table user_small select key from testsrc limit 100;

set hive.auto.convert.join.noconditionaltask.size = 200;
explain extended SELECT
     `dim_pay_date`.`date`
    , `deal`.`dealid`
FROM `orderpayment_small` `orderpayment`
JOIN `orderpayment_small` `dim_pay_date` ON `dim_pay_date`.`date` = `orderpayment`.`date`
JOIN `orderpayment_small` `deal` ON `deal`.`dealid` = `orderpayment`.`dealid`
JOIN `orderpayment_small` `order_city` ON `order_city`.`cityid` = `orderpayment`.`cityid`
JOIN `user_small` `user` ON `user`.`userid` = `orderpayment`.`userid`
limit 5;

SELECT
     `dim_pay_date`.`date`
    , `deal`.`dealid`
FROM `orderpayment_small` `orderpayment`
JOIN `orderpayment_small` `dim_pay_date` ON `dim_pay_date`.`date` = `orderpayment`.`date`
JOIN `orderpayment_small` `deal` ON `deal`.`dealid` = `orderpayment`.`dealid`
JOIN `orderpayment_small` `order_city` ON `order_city`.`cityid` = `orderpayment`.`cityid`
JOIN `user_small` `user` ON `user`.`userid` = `orderpayment`.`userid`
limit 5;
