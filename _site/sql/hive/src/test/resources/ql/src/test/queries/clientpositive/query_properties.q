set hive.exec.post.hooks = org.apache.hadoop.hive.ql.hooks.CheckQueryPropertiesHook;

select * from src a join src b on a.key = b.key limit 0;
select * from src group by src.key, src.value limit 0;
select * from src order by src.key limit 0;
select * from src sort by src.key limit 0;
select a.key, sum(b.value) from src a join src b on a.key = b.key group by a.key limit 0;
select transform(*) using 'cat' from src limit 0;
select * from src distribute by src.key limit 0;
select * from src cluster by src.key limit 0;

select key, sum(value) from (select a.key as key, b.value as value from src a join src b on a.key = b.key) c group by key limit 0;
select * from src a join src b on a.key = b.key order by a.key limit 0;
select * from src a join src b on a.key = b.key distribute by a.key sort by a.key, b.value limit 0;

