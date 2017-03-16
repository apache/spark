SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapreduce.input.fileinputformat.split.minsize=1000;
SET mapreduce.input.fileinputformat.split.maxsize=5000;

create table newtypesorc(c char(10), v varchar(10), d decimal(5,3), da date) stored as orc tblproperties("orc.stripe.size"="16777216"); 

insert overwrite table newtypesorc select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), 0.22, cast("1970-02-20" as date) from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), 11.22, cast("1970-02-27" as date) from src src2) uniontbl;

set hive.optimize.index.filter=false;

-- varchar data types (EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_EQUALS, IN, BETWEEN tests)
select sum(hash(*)) from newtypesorc where v="bee";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where v="bee";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where v!="bee";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where v!="bee";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where v<"world";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where v<"world";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where v<="world";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where v<="world";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where v="bee   ";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where v="bee   ";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where v in ("bee", "orange");

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where v in ("bee", "orange");

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where v in ("bee", "world");

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where v in ("bee", "world");

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where v in ("orange");

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where v in ("orange");

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where v between "bee" and "orange";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where v between "bee" and "orange";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where v between "bee" and "zombie";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where v between "bee" and "zombie";

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where v between "orange" and "pine";

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where v between "orange" and "pine";

