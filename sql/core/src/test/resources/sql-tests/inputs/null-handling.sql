-- Create a test table with data
create table t1(a int, b int, c int) using parquet;
insert into t1 values(1,0,0);
insert into t1 values(2,0,1);
insert into t1 values(3,1,0);
insert into t1 values(4,1,1);
insert into t1 values(5,null,0);
insert into t1 values(6,null,1);
insert into t1 values(7,null,null);

-- Adding anything to null gives null
select a, b+c from t1;

-- Multiplying null by zero gives null
select a+10, b*0 from t1;

-- nulls are NOT distinct in SELECT DISTINCT
select distinct b from t1;

-- nulls are NOT distinct in UNION
select b from t1 union select b from t1;

-- CASE WHEN null THEN 1 ELSE 0 END is 0
select a+20, case b when c then 1 else 0 end from t1;
select a+30, case c when b then 1 else 0 end from t1;
select a+40, case when b<>0 then 1 else 0 end from t1;
select a+50, case when not b<>0 then 1 else 0 end from t1;
select a+60, case when b<>0 and c<>0 then 1 else 0 end from t1;

-- "not (null AND false)" is true
select a+70, case when not (b<>0 and c<>0) then 1 else 0 end from t1;

-- "null OR true" is true
select a+80, case when b<>0 or c<>0 then 1 else 0 end from t1;
select a+90, case when not (b<>0 or c<>0) then 1 else 0 end from t1;

-- null with aggregate operators
select count(*), count(b), sum(b), avg(b), min(b), max(b) from t1;

-- Check the behavior of NULLs in WHERE clauses
select a+100 from t1 where b<10;
select a+110 from t1 where not b>10;
select a+120 from t1 where b<10 OR c=1;
select a+130 from t1 where b<10 AND c=1;
select a+140 from t1 where not (b<10 AND c=1);
select a+150 from t1 where not (c=1 AND b<10);

drop table t1;
