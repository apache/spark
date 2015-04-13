set hive.auto.convert.join=false;
set hive.optimize.skewjoin=true;

explain
create table noskew as select a.* from src a join src b on a.key=b.key order by a.key limit 30;

create table noskew as select a.* from src a join src b on a.key=b.key order by a.key limit 30;

select * from noskew;
