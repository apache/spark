show partitions srcpart;



create table if not exists nzhang_part7 like srcpart;
describe extended nzhang_part7;


insert overwrite table nzhang_part7 partition (ds='2010-03-03', hr='12') select key, value from srcpart where ds = '2008-04-08' and hr = '12';

show partitions nzhang_part7;

select * from nzhang_part7 where ds is not null and hr is not null;

