-- A test suite for IN predicate subquery
-- It includes correlated and non-correlated cases.

-- create table with different data type

CREATE DATABASE indb;
CREATE TABLE t1(t1a String, t1b Int. t1c smallint, t1d double, t1e DECIMAL, t1f date, t1g float, t1h Short,
t1i Short, t1j TIMESTAMP, t1k TIMEZONE_ ) ;
CREATE TABLE t2(t2a String, t2b Int. t2c smallint, t2d double, t2e DECIMAL, t2f date, t2g float, t2h Short,
t2i Short, t2j TIMESTAMP, t2k TIMEZONE_ ) ;

CREATE TABLE t3(t3a String, t3b Int) PARTITIONED BY (t3c String, t3d String);
CREATE TEMPORARY VIEW t4(t4a int, t4b String, t4c double, t4d DECIMAL, t4e date, t4f float, t4h short) USING parquet;
CREATE TEMPORARY VIEW t5(t4a int, t4b String, t4c double, t4d DECIMAL, t4e date, t4f float, t4h short) USING orc;

CREATE GLOBAL TEMP VIEW t6 AS SELECT 1 as col1;

-- create CTE with in predicate subquery investigate
create temporary view t as select * from values 0, 1, 2 as t(id);
create temporary view t2 as select * from values 0, 1 as t(id);

-- WITH clause should not fall into infinite loop by referencing self
WITH cte_t1 AS (SELECT 1 FROM s) SELECT * FROM s;

-- WITH clause should reference the base table
WITH cte_t2 AS (SELECT 1 FROM t) SELECT * FROM t;

-- WITH clause should not allow cross reference
WITH cte_t3 AS (SELECT 1 FROM s2), s2 AS (SELECT 1 FROM s1) SELECT * FROM s1, s2;

-- WITH clause should reference the previous CTE
WITH cte_t4 AS (SELECT * FROM t2), t2 AS (SELECT 2 FROM t1) SELECT * FROM t1 in (t1 t2);

-- correlated in subquery
-- simple select
select * from t1 where t1a in (select t2a from in_t2)
select * from t1 where t1b in (select t2b from in_t2 where in_t1.t1a = in_t2.t2a)
select t1.t1a, t1.t1b from t1 where t1c in (select t2b from t2 where t1.t1a = t2.t2a)
-- add group by
select * from t1 where t1a in (select t2a from in_t2) group by t1a
select * from t1 where t1b in (select t2b from in_t2 where in_t1.t1a = in_t2.t2a) group by t1b
select t1.t1a, t1.t1b from t1 where t1c in (select t2b from t2 where t1.t1a = t2.t2a) group by t1a

-- add order by

select * from t1 where t1a in (select t2a from in_t2) order by t1a
select * from t1 where t1b in (select t2b from in_t2 where in_t1.t1a = in_t2.t2a) order by t1b
select t1.t1a, t1.t1b from t1 where t1c in (select t2b from t2 where t1.t1a = t2.t2a) order by t1a

-- add limit into the subquery (inner query block)

select * from t1 where t1a in (select t2a from in_t2 limit 2)
select * from t1 where t1b in (select t2b from in_t2 where in_t1.t1a = in_t2.t2a limit 2)
select t1.t1a, t1.t1b from t1 where t1c in (select t2b from t2 where t1.t1a = t2.t2a limit 2)

-- add having clause in the subquery (inner query block)
select * from t1 where t1a in (select t2a from in_t2 having t2a > 2)
select * from t1 where t1b in (select t2b from in_t2 where in_t1.t1a = in_t2.t2a having t2a > 2)
select t1.t1a, t1.t1b from t1 where t1c in (select t2b from t2 where t1.t1a = t2.t2a having t2a > 2)


-- add radom in the out query predicate
select random(t1.t1a) from t1 where t1c in (select t2b from t2 where t1.t1a = t2.t2a)

select random(t1.t1a), t1.t1b from t1 where t1c in (select t2b from t2 where t1.t1a = t2.t2a having t2a > 2)


-- with three table and different join at inner query
select * from t1 where t1a in (select t2a from in_t2) natural join t3 where t1a = 2;
select * from t1 where t1a in (select t2a from in_t2) left join t3 order by t1a;
select * from t1 where t1a in (select t2a from in_t2) right join t3 order by t1a, t1b;
select count(*) from t1 where t1a in (select t2a from t2) natural full outer join t3 ;
select * from t1 where t1b in (select t2b from t2 natural join t3 where t3a = t1b)
select * from t1 where t1b in (select t2b from t2 left join t3 order by t2b)
select * from t1 where t1b in (select t2b from t2 right join t3)

select t1.t1a, t1.t1b from t1 where t1c in (select t2b from t2 left join t3 order by t2b)

-- with union in the subquery
SELECT *
FROM t1 where t1.t1a in (select t3a from
                                (SELECT * FROM t2
                                 UNION ALL
                                 SELECT * FROM t2) as t3);

SELECT *
FROM t1 where t1.t1a in (select t3a from
                            (SELECT * FROM t2
                             UNION ALL
                             SELECT * FROM t2) as t3 where t1.t1a = t3.t3a);

-- test on different table type to do ?

-- test on partition table to do ?

-- test on parquet table to do?

-- test on orc table to do ?

-- should we test table from jdbc data source?

-- table with cte
select * from t1 where t1a in (select ctet1.id from ctet1 where ctet1.id = t1.t1a)
select t1.t1b, t1.t1c from t1 where t1a in (select ctet1.id from ctet1 where ctet1.id = t1.t1a)

-- table with null to do?

-- duplicate row and null value

-- with inline table to do ?






