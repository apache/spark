drop table char_nested_1;
drop table char_nested_array;
drop table char_nested_map;
drop table char_nested_struct;
drop table char_nested_cta;
drop table char_nested_view;

create table char_nested_1 (key int, value char(20));
insert overwrite table char_nested_1
  select key, value from src order by key limit 1;

-- arrays
create table char_nested_array (c1 array<char(20)>);
insert overwrite table char_nested_array
  select array(value, value) from char_nested_1;
describe char_nested_array;
select * from char_nested_array;

-- maps
create table char_nested_map (c1 map<int, char(20)>);
insert overwrite table char_nested_map
  select map(key, value) from char_nested_1;
describe char_nested_map;
select * from char_nested_map;

-- structs
create table char_nested_struct (c1 struct<a:int, b:char(20), c:string>);
insert overwrite table char_nested_struct
  select named_struct('a', key,
                      'b', value,
                      'c', cast(value as string))
  from char_nested_1;
describe char_nested_struct;
select * from char_nested_struct;

-- nested type with create table as
create table char_nested_cta as 
  select * from char_nested_struct;
describe char_nested_cta;
select * from char_nested_cta;

-- nested type with view
create table char_nested_view as 
  select * from char_nested_struct;
describe char_nested_view;
select * from char_nested_view;

drop table char_nested_1;
drop table char_nested_array;
drop table char_nested_map;
drop table char_nested_struct;
drop table char_nested_cta;
drop table char_nested_view;
