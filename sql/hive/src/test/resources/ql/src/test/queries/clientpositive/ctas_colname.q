-- HIVE-4392, column aliases from expressionRR (GBY, etc.) are not valid name for table
-- use internal name as column name instead

-- group by
explain
create table summary as select *, sum(key), count(value) from src;
create table summary as select *, sum(key), count(value) from src;
describe formatted summary;
select * from summary order by `_col0`, `_col1`, `_c1`, `_c2`;

-- window functions
explain
create table x4 as select *, rank() over(partition by key order by value) as rr from src1;
create table x4 as select *, rank() over(partition by key order by value) as rr from src1;
describe formatted x4;
select * from x4 order by key, value, rr;

explain
create table x5 as select *, lead(key,1) over(partition by key order by value) as lead1 from src limit 20;
create table x5 as select *, lead(key,1) over(partition by key order by value) as lead1 from src limit 20;
describe formatted x5;
select * from x5 order by key, value, lead1;

-- sub queries
explain
create table x6 as select * from (select *, max(key) from src1) a;
create table x6 as select * from (select *, max(key) from src1) a;
describe formatted x6;
select * from x6 order by `_col0`, `_c1`;

explain
create table x7 as select * from (select * from src group by key) a;
create table x7 as select * from (select * from src group by key) a;
describe formatted x7;
select * from x7 order by `_col0`;

explain
create table x8 as select * from (select * from src group by key having key < 9) a;
create table x8 as select * from (select * from src group by key having key < 9) a;
describe formatted x8;
select * from x8 order by `_col0`;

explain
create table x9 as select * from (select max(value),key from src group by key having key < 9 AND max(value) IS NOT NULL) a;
create table x9 as select * from (select max(value),key from src group by key having key < 9 AND max(value) IS NOT NULL) a;
describe formatted x9;
select * from x9 order by key, `_c0`;

