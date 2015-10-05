create table json_t (key string, jstring string);

insert overwrite table json_t
select * from (
  select '1', '{"f1": "value1", "f2": "value2", "f3": 3, "f5": 5.23}' from src tablesample (1 rows)
  union all
  select '2', '{"f1": "value12", "f3": "value3", "f2": 2, "f4": 4.01}' from src tablesample (1 rows)
  union all
  select '3', '{"f1": "value13", "f4": "value44", "f3": "value33", "f2": 2, "f5": 5.01}' from src tablesample (1 rows)
  union all
  select '4', cast(null as string) from src tablesample (1 rows)
  union all
  select '5', '{"f1": "", "f5": null}' from src tablesample (1 rows)
  union all
  select '6', '[invalid JSON string]' from src tablesample (1 rows)
) s;

explain 
select a.key, b.* from json_t a lateral view json_tuple(a.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') b as f1, f2, f3, f4, f5 order by a.key;

select a.key, b.* from json_t a lateral view json_tuple(a.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') b as f1, f2, f3, f4, f5 order by a.key;

explain 
select json_tuple(a.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') as (f1, f2, f3, f4, f5) from json_t a order by f1, f2, f3;

select json_tuple(a.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') as (f1, f2, f3, f4, f5) from json_t a order by f1, f2, f3;

explain 
select a.key, b.f2, b.f5 from json_t a lateral view json_tuple(a.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') b as f1, f2, f3, f4, f5 order by a.key;

select a.key, b.f2, b.f5 from json_t a lateral view json_tuple(a.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') b as f1, f2, f3, f4, f5 order by a.key;

explain 
select f2, count(*) from json_t a lateral view json_tuple(a.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') b as f1, f2, f3, f4, f5 where f1 is not null group by f2 order by f2;

select f2, count(*) from json_t a lateral view json_tuple(a.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') b as f1, f2, f3, f4, f5 where f1 is not null group by f2 order by f2;


-- Verify that json_tuple can handle new lines in JSON values

CREATE TABLE dest1(c1 STRING) STORED AS RCFILE;

INSERT OVERWRITE TABLE dest1 SELECT '{"a":"b\nc"}' FROM src tablesample (1 rows);

SELECT * FROM dest1;

SELECT json FROM dest1 a LATERAL VIEW json_tuple(c1, 'a') b AS json;