-- HIVE-2901
select x from (select value as x from src union all select NULL as x from src)a limit 10;
