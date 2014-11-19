
create table nestedcomplex (
simple_int int,
max_nested_array  array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<int>>>>>>>>>>>>>>>>>>>>>>>,
max_nested_map    array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<map<string,string>>>>>>>>>>>>>>>>>>>>>>,
max_nested_struct array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<struct<s:string, i:bigint>>>>>>>>>>>>>>>>>>>>>>>,
simple_string string)

;


-- This should fail in as extended nesting levels are not enabled using the serdeproperty hive.serialization.extend.nesting.levels
load data local inpath '../../data/files/nested_complex.txt' overwrite into table nestedcomplex;

select * from nestedcomplex sort by simple_int;
