--HIVE 3226
drop table array_valued_src;
create table array_valued_src (key string, value array<string>);
insert overwrite table array_valued_src select key, array(value) from src;

-- replace sel(*) to sel(exprs) for reflecting CP result properly
explain select count(val) from (select a.key as key, b.value as array_val from src a join array_valued_src b on a.key=b.key) i lateral view explode (array_val) c as val;
select count(val) from (select a.key as key, b.value as array_val from src a join array_valued_src b on a.key=b.key) i lateral view explode (array_val) c as val;
