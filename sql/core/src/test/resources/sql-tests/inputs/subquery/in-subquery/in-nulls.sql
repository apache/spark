create table t0(t0a int not null, t0b int) using parquet;
insert into t0 values (1, 0);

create table t1(t1a int, t1b int) using parquet;
insert into t1 values (null, 1);

create table t2(t2a int, t2b int) using parquet;
insert into t2 values (2, 2);

-- SPARK-43413: Tests for IN subquery nullability
select * from t0 where t0a in (select t1a from t1);
select * from t0 where (t0a in (select t1a from t1)) <=> true;

select * from t0 left join t2 on (t0a IN (select t1a from t1)) is null;
select * from t0 left join t2 on (t0a IN (select t1a from t1)) <=> true;
