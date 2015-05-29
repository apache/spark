set hive.fetch.task.aggr=true;

explain
select count(key),sum(key),avg(key),min(key),max(key),std(key),variance(key) from src;

select count(key),sum(key),avg(key),min(key),max(key),std(key),variance(key) from src;
