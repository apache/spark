set hive.map.aggr=true;
set hive.groupby.skewindata=true;

explain
select x.key, count(1) from src x where x.key > 9999 group by x.key;

select x.key, count(1) from src x where x.key > 9999 group by x.key;

set hive.map.aggr=true;
set hive.groupby.skewindata=false;

explain
select x.key, count(1) from src x where x.key > 9999 group by x.key;

select x.key, count(1) from src x where x.key > 9999 group by x.key;

set hive.map.aggr=false;
set hive.groupby.skewindata=true;

explain
select x.key, count(1) from src x where x.key > 9999 group by x.key;

select x.key, count(1) from src x where x.key > 9999 group by x.key;

set hive.map.aggr=false;
set hive.groupby.skewindata=false;

explain
select x.key, count(1) from src x where x.key > 9999 group by x.key;

select x.key, count(1) from src x where x.key > 9999 group by x.key;
