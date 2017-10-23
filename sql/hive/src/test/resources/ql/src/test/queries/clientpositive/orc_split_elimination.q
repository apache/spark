create table orc_split_elim (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) stored as orc;

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_split_elim;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapreduce.input.fileinputformat.split.minsize=1000;
SET mapreduce.input.fileinputformat.split.maxsize=5000;
SET hive.optimize.index.filter=false;

-- The above table will have 5 splits with the followings stats
--  Stripe 1:
--    Column 0: count: 5000
--    Column 1: count: 5000 min: 2 max: 100 sum: 499902
--    Column 2: count: 5000 min: foo max: zebra sum: 24998
--    Column 3: count: 5000 min: 0.8 max: 8.0 sum: 39992.8
--    Column 4: count: 5000 min: 0 max: 1.2 sum: 1.2
--    Column 5: count: 5000
--  Stripe 2:
--    Column 0: count: 5000
--    Column 1: count: 5000 min: 13 max: 100 sum: 499913
--    Column 2: count: 5000 min: bar max: zebra sum: 24998
--    Column 3: count: 5000 min: 8.0 max: 80.0 sum: 40072.0
--    Column 4: count: 5000 min: 0 max: 2.2 sum: 2.2
--    Column 5: count: 5000
--  Stripe 3:
--    Column 0: count: 5000
--    Column 1: count: 5000 min: 29 max: 100 sum: 499929
--    Column 2: count: 5000 min: cat max: zebra sum: 24998
--    Column 3: count: 5000 min: 8.0 max: 8.0 sum: 40000.0
--    Column 4: count: 5000 min: 0 max: 3.3 sum: 3.3
--    Column 5: count: 5000
--  Stripe 4:
--    Column 0: count: 5000
--    Column 1: count: 5000 min: 70 max: 100 sum: 499970
--    Column 2: count: 5000 min: dog max: zebra sum: 24998
--    Column 3: count: 5000 min: 1.8 max: 8.0 sum: 39993.8
--    Column 4: count: 5000 min: 0 max: 4.4 sum: 4.4
--    Column 5: count: 5000
--  Stripe 5:
--    Column 0: count: 5000
--    Column 1: count: 5000 min: 5 max: 100 sum: 499905
--    Column 2: count: 5000 min: eat max: zebra sum: 24998
--    Column 3: count: 5000 min: 0.8 max: 8.0 sum: 39992.8
--    Column 4: count: 5000 min: 0 max: 5.5 sum: 5.5
--    Column 5: count: 5000

-- 5 mappers
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=0;

SET hive.optimize.index.filter=true;
-- 0 mapper
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=0;
SET hive.optimize.index.filter=false;

-- 5 mappers. count should be 0
select count(*) from orc_split_elim where userid<=0;

SET hive.optimize.index.filter=true;
-- 0 mapper
select count(*) from orc_split_elim where userid<=0;
SET hive.optimize.index.filter=false;

-- 5 mappers
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=2 order by userid;

SET hive.optimize.index.filter=true;
-- 1 mapper
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=2 order by userid;
SET hive.optimize.index.filter=false;

-- 5 mappers
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=5 order by userid;

SET hive.optimize.index.filter=true;
-- 2 mappers
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=5 order by userid;
SET hive.optimize.index.filter=false;

-- 5 mappers
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=13 order by userid;

SET hive.optimize.index.filter=true;
-- 3 mappers
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=13 order by userid;
SET hive.optimize.index.filter=false;

-- 5 mappers
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=29 order by userid;

SET hive.optimize.index.filter=true;
-- 4 mappers
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=29 order by userid;
SET hive.optimize.index.filter=false;

-- 5 mappers
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=70 order by userid;

SET hive.optimize.index.filter=true;
-- 5 mappers
select userid,string1,subtype,decimal1,ts from orc_split_elim where userid<=70 order by userid;
SET hive.optimize.index.filter=false;

-- partitioned table
create table orc_split_elim_part (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) partitioned by (country string, year int) stored as orc;

alter table orc_split_elim_part add partition(country='us', year=2000);
alter table orc_split_elim_part add partition(country='us', year=2001);

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_split_elim_part partition(country='us', year=2000);
load data local inpath '../../data/files/orc_split_elim.orc' into table orc_split_elim_part partition(country='us', year=2001);

-- 10 mapper - no split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=2 and country='us'order by userid;

SET hive.optimize.index.filter=true;
-- 2 mapper - split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=2 and country='us' order by userid;
SET hive.optimize.index.filter=false;

-- 10 mapper - no split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=2 and country='us' and (year=2000 or year=2001) order by userid;

SET hive.optimize.index.filter=true;
-- 2 mapper - split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=2 and country='us' and (year=2000 or year=2001) order by userid;
SET hive.optimize.index.filter=false;

-- 10 mapper - no split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=2 and country='us' and year=2000 order by userid;

SET hive.optimize.index.filter=true;
-- 1 mapper - split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=2 and country='us' and year=2000 order by userid;
SET hive.optimize.index.filter=false;

-- 10 mapper - no split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=5 and country='us' order by userid;

SET hive.optimize.index.filter=true;
-- 4 mapper - split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=5 and country='us' order by userid;
SET hive.optimize.index.filter=false;

-- 10 mapper - no split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=5 and country='us' and (year=2000 or year=2001) order by userid;

SET hive.optimize.index.filter=true;
-- 4 mapper - split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=5 and country='us' and (year=2000 or year=2001) order by userid;
SET hive.optimize.index.filter=false;

-- 10 mapper - no split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=5 and country='us' and year=2000 order by userid;

SET hive.optimize.index.filter=true;
-- 2 mapper - split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=5 and country='us' and year=2000 order by userid;
SET hive.optimize.index.filter=false;

-- 0 mapper - no split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=70 and country='in' order by userid;
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=70 and country='us' and year=2002 order by userid;

SET hive.optimize.index.filter=true;
-- 0 mapper - split elimination
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=70 and country='in' order by userid;
select userid,string1,subtype,decimal1,ts from orc_split_elim_part where userid<=70 and country='us' and year=2002 order by userid;
SET hive.optimize.index.filter=false;
