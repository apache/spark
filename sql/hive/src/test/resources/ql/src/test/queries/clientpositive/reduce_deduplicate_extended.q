set hive.optimize.reducededuplication=true;
set hive.optimize.reducededuplication.min.reducer=1;
set hive.map.aggr=true;

-- HIVE-2340 deduplicate RS followed by RS
-- hive.optimize.reducededuplication : wherther using this optimization
-- hive.optimize.reducededuplication.min.reducer : number of reducer of deduped RS should be this at least

-- RS-mGBY-RS-rGBY
explain select key, sum(key) from (select * from src distribute by key sort by key, value) Q1 group by key;
explain select key, sum(key), lower(value) from (select * from src order by key) Q1 group by key, lower(value);
explain select key, sum(key), (X + 1) from (select key, (value + 1) as X from src order by key) Q1 group by key, (X + 1);
-- mGBY-RS-rGBY-RS
explain select key, sum(key) as value from src group by key order by key, value;
-- RS-JOIN-mGBY-RS-rGBY
explain select src.key, sum(src.key) FROM src JOIN src1 ON src.key = src1.key group by src.key, src.value;
-- RS-JOIN-RS
explain select src.key, src.value FROM src JOIN src1 ON src.key = src1.key order by src.key, src.value;
-- mGBY-RS-rGBY-mGBY-RS-rGBY
explain from (select key, value from src group by key, value) s select s.key group by s.key;
explain select key, count(distinct value) from (select key, value from src group by key, value) t group by key;

select key, sum(key) from (select * from src distribute by key sort by key, value) Q1 group by key;
select key, sum(key), lower(value) from (select * from src order by key) Q1 group by key, lower(value);
select key, sum(key), (X + 1) from (select key, (value + 1) as X from src order by key) Q1 group by key, (X + 1);
select key, sum(key) as value from src group by key order by key, value;
select src.key, sum(src.key) FROM src JOIN src1 ON src.key = src1.key group by src.key, src.value;
select src.key, src.value FROM src JOIN src1 ON src.key = src1.key order by src.key, src.value;
from (select key, value from src group by key, value) s select s.key group by s.key;
select key, count(distinct value) from (select key, value from src group by key, value) t group by key;

set hive.map.aggr=false;

-- RS-RS-GBY
explain select key, sum(key) from (select * from src distribute by key sort by key, value) Q1 group by key;
explain select key, sum(key), lower(value) from (select * from src order by key) Q1 group by key, lower(value);
explain select key, sum(key), (X + 1) from (select key, (value + 1) as X from src order by key) Q1 group by key, (X + 1);
-- RS-GBY-RS
explain select key, sum(key) as value from src group by key order by key, value;
-- RS-JOIN-RS-GBY
explain select src.key, sum(src.key) FROM src JOIN src1 ON src.key = src1.key group by src.key, src.value;
-- RS-JOIN-RS
explain select src.key, src.value FROM src JOIN src1 ON src.key = src1.key order by src.key, src.value;
-- RS-GBY-RS-GBY
explain from (select key, value from src group by key, value) s select s.key group by s.key;
explain select key, count(distinct value) from (select key, value from src group by key, value) t group by key;

select key, sum(key) from (select * from src distribute by key sort by key, value) Q1 group by key;
select key, sum(key), lower(value) from (select * from src order by key) Q1 group by key, lower(value);
select key, sum(key), (X + 1) from (select key, (value + 1) as X from src order by key) Q1 group by key, (X + 1);
select key, sum(key) as value from src group by key order by key, value;
select src.key, sum(src.key) FROM src JOIN src1 ON src.key = src1.key group by src.key, src.value;
select src.key, src.value FROM src JOIN src1 ON src.key = src1.key order by src.key, src.value;
from (select key, value from src group by key, value) s select s.key group by s.key;
select key, count(distinct value) from (select key, value from src group by key, value) t group by key;
