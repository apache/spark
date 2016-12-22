-- A test suite for simple IN predicate subquery
-- It includes correlated cases.

-- tables and data types

CREATE DATABASE indb;
CREATE TABLE t1(t1a String, t1b Short, t1c Int, t1d Long, t1e float, t1f double, t1g DECIMAL, t1h TIMESTAMP, t1i Date)
using parquet;
CREATE TABLE t2(t2a String, t2b Short, t2c Int, t2d Long, t2e float, t2f double, t2g DECIMAL, t2h TIMESTAMP, t2i Date)
using parquet;
CREATE TABLE t3(t3a String, t3b Short, t3c Int, t3d Long, t3e float, t3f double, t3g DECIMAL, t3h TIMESTAMP, t3i Date)
using parquet;

-- insert to tables
INSERT INTO t1 VALUES
 ('t1a', 6, 8, 10, 15, 20, 20.00, timestamp(date("2014-04-04")), date("2014-04-04")),
 ('t1b', 8, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-05-04")),
 ('t1a', 16, 12, 21, 15, 20, 20.00, timestamp(date("2014-06-04")), date("2014-06-04")),
 ('t1a', 16, 12, 10, 15, 20, 20.00, timestamp(date("2014-07-04")), date("2014-07-04")),
 ('t1c', 8, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-05-05")),
 ('t1d', null, 16, 22, 17, 25, 26.00, timestamp(date("2014-06-04")), null),
 ('t1d', null, 16, 19, 17, 25, 26.00, timestamp(date("2014-07-04")), null),
 ('t1e', 10, null, 25, 17, 25, 26.00, timestamp(date("2014-08-04")), date("2014-08-04")),
 ('t1e', 10, null, 19, 17, 25, 26.00, timestamp(date("2014-09-04")), date("2014-09-04")),
 ('t1d', 10, null, 12, 17, 25, 26.00, timestamp(date("2015-05-04")), date("2015-05-04")),
 ('t1a', 6, 8, 10, 15, 20, 20.00, timestamp(date("2014-04-04")), date("2014-04-04")),
 ('t1e', 10, null, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-05-0=4"));

INSERT INTO t2 VALUES
 ('t2a', 6, 12, 14, 15, 20, 20.00, timestamp(date("2014-04-04")), date("2014-04-04")),
 ('t1b', 10, 12, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-05-04")),
 ('t1b', 8, 16, 119, 17, 25, 26.00, timestamp(date("2015-05-04")), date("2015-05-04")),
 ('t1c', 12, 16, 219, 17, 25, 26.00, timestamp(date("2016-05-04")), date("2016-05-04")),
 ('t1b', null, 16, 319, 17, 25, 26.00, timestamp(date("2017-05-04")), null),
 ('t2e', 8, null, 419, 17, 25, 26.00, timestamp(date("2014-06-04")), date("2014-06-04")),
 ('t1f', 19, null, 519, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-05-04")),
 ('t1b', 10, 12, 19, 17, 25, 26.00, timestamp(date("2014-06-04")), date("2014-06-04")),
 ('t1b', 8, 16, 19, 17, 25, 26.00, timestamp(date("2014-07-04")), date("2014-07-04")),
 ('t1c', 12, 16, 19, 17, 25, 26.00, timestamp(date("2014-08-04")), date("2014-08-05")),
 ('t1e', 8, null, 19, 17, 25, 26.00, timestamp(date("2014-09-04")), date("2014-09-04")),
 ('t1f', 19, null, 19, 17, 25, 26.00, timestamp(date("2014-10-04")), date("2014-10-04")),
 ('t1b', null, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), null);

INSERT INTO t3 VALUES
 ('t3a', 6, 12, 110, 15, 20, 20.00, timestamp(date("2014-04-04")), date("2014-04-04")),
 ('t3a', 6, 12, 10, 15, 20, 20.00, timestamp(date("2014-05-04")), date("2014-05-04")),
 ('t1b', 10, 12, 219, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-05-04")),
 ('t1b', 10, 12, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-05-04")),
 ('t1b', 8, 16, 319, 17, 25, 26.00, timestamp(date("2014-06-04")), date("2014-06-04")),
 ('t1b', 8, 16, 19, 17, 25, 26.00, timestamp(date("2014-07-04")), date("2014-07-04")),
 ('t3c', 17, 16, 519, 17, 25, 26.00, timestamp(date("2014-08-04")), date("2014-08-04")),
 ('t3c', 17, 16, 19, 17, 25, 26.00, timestamp(date("2014-09-04")), date("2014-09-05")),
 ('t1b', null, 16, 419, 17, 25, 26.00, timestamp(date("2014-10-04")), null),
 ('t1b', null, 16, 19, 17, 25, 26.00, timestamp(date("2014-11-04")), null),
 ('t3b', 8, null, 719, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-05-04")),
 ('t3b', 8, null, 19, 17, 25, 26.00, timestamp(date("2015-05-04")), date("2015-05-04"));

-- correlated IN subquery
-- simple select
-- TC 01.01
select * from t1 where t1a in (select t2a from t2);
-- TC 01.02
select * from t1 where t1b in (select t2b from t2 where t1a = t2a);
-- TC 01.03
select t1a, t1b from t1 where t1c in (select t2b from t2 where t1a != t2a);
-- TC 01.04
select t1a, t1b from t1 where t1c in (select t2b from t2 where t1a = t2a or t1b > t2b);
-- TC 01.05
select t1a, t1b from t1 where t1c in (select t2b from t2 where t2i in (select t3i from t3 where t2c = t3c));
-- TC 01.06
select t1a, t1b from t1 where t1c in (select t2b from t2 where t2a in
                      (select t3a from t3 where t2c = t3c and t2b is not NULL));
-- simple select for NOT IN
-- TC 01.07
select distinct(t1a), t1b, t1h from t1 where t1a not in (select t2a from t2);
-- TC 01.08, comment out pending on SPARK-18966
--select t1d, t1h, t1i from t1 where t1d not in (select t2d from t2 where t2h > t1h or t2i > t1i);
-- TC 01.09
select distinct(t1a), t1b from t1 where t1b not in (select t2b from t2 where t1a < t2a and t2b > 8);
-- TC 01.10, comment out pending on SPARK-18966
--select t1a, t1b from t1 where t1c not in (select t2b from t2 where t2a not in
--                      (select t3a from t3 where t2c = t3c and t2b is NULL));
-- TC 01.11
select t1a, t1b from t1 where t1h not in (select t2h from t2 where t2a = t1a) and t1b not in (
                      (select min(t3b) from t3 where t3d = t1d));

-- Clean Up
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
USE default;
DROP DATABASE indb;