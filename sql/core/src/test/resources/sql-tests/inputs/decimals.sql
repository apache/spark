-- tests for decimals handling in operations
-- Spark draws its inspiration byt Hive implementation
create table decimals_test(id int, a decimal(38,18), b decimal(38,18)) using parquet;

insert into decimals_test values(1, 100.0, 999.0);
insert into decimals_test values(2, 12345.123, 12345.123);
insert into decimals_test values(3, 0.1234567891011, 1234.1);
insert into decimals_test values(4, 123456789123456789.0, 1.123456789123456789);

-- test decimal operations
select id, a+b, a-b, a*b, a/b from decimals_test order by id;

-- test operations between decimals and constants
select id, a*10, b/10 from decimals_test order by id;

drop table decimals_test;