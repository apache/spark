

set hive.map.aggr=false;
set hive.groupby.skewindata=false;
explain select key, count(*) from src b group by b.key;
explain select b.key, count(*) from src b group by key;

set hive.map.aggr=false;
set hive.groupby.skewindata=true;
explain select key, count(*) from src b group by b.key;
explain select b.key, count(*) from src b group by key;

set hive.map.aggr=true;
set hive.groupby.skewindata=false;
explain select key, count(*) from src b group by b.key;
explain select b.key, count(*) from src b group by key;

set hive.map.aggr=true;
set hive.groupby.skewindata=true;
explain select key, count(*) from src b group by b.key;
explain select b.key, count(*) from src b group by key;

-- windowing after group by
select key, count(*), rank() over(order by count(*))
from src b
where key < '12'
group by b.key
order by b.key;

-- having after group by
select key, count(*)
from src b
group by b.key
having key < '12'
order by b.key;

-- having and windowing
select key, count(*), rank() over(order by count(*))
from src b
group by b.key
having key < '12'
order by b.key
;

explain
select key, count(*), rank() over(order by count(*))
from src b
group by b.key
having key < '12'
;

-- order by
select key 
from src t 
where key < '12'
group by t.key 
order by t.key;

-- cluster by
EXPLAIN
SELECT x.key, x.value as key FROM SRC x CLUSTER BY key;
