set hive.map.aggr=true;
set hive.groupby.skewindata=true;

explain
select count(1) from src x where x.key > 9999;

select count(1) from src x where x.key > 9999;

set hive.map.aggr=true;
set hive.groupby.skewindata=false;

explain
select count(1) from src x where x.key > 9999;

select count(1) from src x where x.key > 9999;

set hive.map.aggr=false;
set hive.groupby.skewindata=true;

explain
select count(1) from src x where x.key > 9999;

select count(1) from src x where x.key > 9999;

set hive.map.aggr=false;
set hive.groupby.skewindata=false;

explain
select count(1) from src x where x.key > 9999;

select count(1) from src x where x.key > 9999;
