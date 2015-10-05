SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table newtypesorc(c char(10), v varchar(10), d decimal(5,3), da date) stored as orc tblproperties("orc.stripe.size"="16777216"); 

insert overwrite table newtypesorc select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), 0.22, cast("1970-02-20" as date) from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), 11.22, cast("1970-02-27" as date) from src src2) uniontbl;

set hive.optimize.index.filter=false;

-- char data types (EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_EQUALS, IN, BETWEEN tests)
select sum(hash(*)) from newtypesorc where c="apple";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where c="apple";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where c!="apple";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where c!="apple";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where c<"hello";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where c<"hello";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where c<="hello";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where c<="hello";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where c="apple ";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where c="apple ";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where c in ("apple", "carrot");

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where c in ("apple", "carrot");

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where c in ("apple", "hello");

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where c in ("apple", "hello");

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where c in ("carrot");

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where c in ("carrot");

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where c between "apple" and "carrot";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where c between "apple" and "carrot";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where c between "apple" and "zombie";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where c between "apple" and "zombie";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where c between "carrot" and "carrot1";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where c between "carrot" and "carrot1";

