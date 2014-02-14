explain
select concat(*),array(*) from src where key < 100 limit 10;

select concat(*),array(*) from src where key < 100 limit 10;

-- The order of columns is decided by row schema of prev operator
-- Like join which has two or more aliases, it's from left most aias to right aliases.

explain
select stack(2, *) as (e1,e2,e3) from (
  select concat(*), concat(a.*), concat(b.*), concat(a.*, b.key), concat(a.key, b.*)
  from src a join src b on a.key+1=b.key where a.key < 100) x limit 10;

select stack(2, *) as (e1,e2,e3) from (
  select concat(*), concat(a.*), concat(b.*), concat(a.*, b.key), concat(a.key, b.*)
  from src a join src b on a.key+1=b.key where a.key < 100) x limit 10;

-- HIVE-4181 TOK_FUNCTIONSTAR for UDTF
create table allcolref as select array(key, value) from src;
explain select explode(*) as x from allcolref limit 10;
select explode(*) as x from allcolref limit 10;
