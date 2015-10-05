-- HIVE-2901
select x from (select value as x from src union all select NULL as x from src)a limit 10;

-- HIVE-4837
select * from (select null as N from src1 group by key UNION ALL select null as N from src1 group by key ) a;
