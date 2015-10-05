drop table varchar_nested_1;
drop table varchar_nested_array;
drop table varchar_nested_map;
drop table varchar_nested_struct;
drop table varchar_nested_cta;
drop table varchar_nested_view;

create table varchar_nested_1 (key int, value varchar(20));
insert overwrite table varchar_nested_1
  select key, value from src order by key limit 1;

-- arrays
create table varchar_nested_array (c1 array<varchar(20)>);
insert overwrite table varchar_nested_array
  select array(value, value) from varchar_nested_1;
describe varchar_nested_array;
select * from varchar_nested_array;

-- maps
create table varchar_nested_map (c1 map<int, varchar(20)>);
insert overwrite table varchar_nested_map
  select map(key, value) from varchar_nested_1;
describe varchar_nested_map;
select * from varchar_nested_map;

-- structs
create table varchar_nested_struct (c1 struct<a:int, b:varchar(20), c:string>);
insert overwrite table varchar_nested_struct
  select named_struct('a', key,
                      'b', value,
                      'c', cast(value as string))
  from varchar_nested_1;
describe varchar_nested_struct;
select * from varchar_nested_struct;

-- nested type with create table as
create table varchar_nested_cta as 
  select * from varchar_nested_struct;
describe varchar_nested_cta;
select * from varchar_nested_cta;

-- nested type with view
create table varchar_nested_view as 
  select * from varchar_nested_struct;
describe varchar_nested_view;
select * from varchar_nested_view;

drop table varchar_nested_1;
drop table varchar_nested_array;
drop table varchar_nested_map;
drop table varchar_nested_struct;
drop table varchar_nested_cta;
drop table varchar_nested_view;
