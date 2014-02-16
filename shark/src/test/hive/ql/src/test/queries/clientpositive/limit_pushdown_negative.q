set hive.limit.pushdown.memory.usage=0.3f;

-- negative, RS + join
explain select * from src a join src b on a.key=b.key limit 20;

-- negative, RS + filter
explain select value, sum(key) as sum from src group by value having sum > 100 limit 20;

-- negative, RS + lateral view
explain select key, L.* from (select * from src order by key) a lateral view explode(array(value, value)) L as v limit 10;

-- negative, RS + forward + multi-groupby
CREATE TABLE dest_2(key STRING, c1 INT);
CREATE TABLE dest_3(key STRING, c1 INT);

EXPLAIN FROM src
INSERT OVERWRITE TABLE dest_2 SELECT value, sum(key) GROUP BY value
INSERT OVERWRITE TABLE dest_3 SELECT value, sum(key) GROUP BY value limit 20;

-- nagative, multi distinct
explain
select count(distinct key)+count(distinct value) from src limit 20;
