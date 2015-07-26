SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table newtypesorc(c char(10), v varchar(10), d decimal(5,3), da date) stored as orc tblproperties("orc.stripe.size"="16777216"); 

insert overwrite table newtypesorc select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), 0.22, cast("1970-02-20" as date) from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), 11.22, cast("1970-02-27" as date) from src src2) uniontbl;

-- date data types (EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_EQUALS, IN, BETWEEN tests)
select sum(hash(*)) from newtypesorc where da='1970-02-20';

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da='1970-02-20';

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da=cast('1970-02-20' as date);

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da=cast('1970-02-20' as date);

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da=cast('1970-02-20' as varchar(20));

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da=cast('1970-02-20' as varchar(20));

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da!='1970-02-20';

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da!='1970-02-20';

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da<'1970-02-27';

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da<'1970-02-27';

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da<'1970-02-29';

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da<'1970-02-29';

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da<'1970-02-15';

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da<'1970-02-15';

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da<='1970-02-20';

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da<='1970-02-20';

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da<='1970-02-27';

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da<='1970-02-27';

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da in (cast('1970-02-21' as date), cast('1970-02-27' as date));

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da in (cast('1970-02-21' as date), cast('1970-02-27' as date));

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da in (cast('1970-02-20' as date), cast('1970-02-27' as date));

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da in (cast('1970-02-20' as date), cast('1970-02-27' as date));

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da in (cast('1970-02-21' as date), cast('1970-02-22' as date));

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da in (cast('1970-02-21' as date), cast('1970-02-22' as date));

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da between '1970-02-19' and '1970-02-22';

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da between '1970-02-19' and '1970-02-22';

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da between '1970-02-19' and '1970-02-28';

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da between '1970-02-19' and '1970-02-28';

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where da between '1970-02-18' and '1970-02-19';

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where da between '1970-02-18' and '1970-02-19';
