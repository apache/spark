-- A test suite for GROUP BY in parent side, subquery, and both predicate subquery
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
-- GROUP BY in parent side
-- TC 01.01
select t1a, avg(t1b) from t1 where t1a in (select t2a from t2) group by t1a;
-- TC 01.02
select t1a, max(t1b) from t1 where t1b in (select t2b from t2 where t1a = t2a) group by t1a, t1d;
-- TC 01.03
select t1a, t1b from t1 where t1c in (select t2c from t2 where t1a = t2a) group by t1a, t1b;
-- TC 01.04
select t1a, sum(distinct(t1b)) from t1 where t1c in (select t2c from t2 where t1a = t2a) or
t1c in (select t3c from t3 where t1a = t3a) group by t1a, t1c;
-- TC 01.05
select t1a, sum(distinct(t1b)) from t1 where t1c in (select t2c from t2 where t1a = t2a) and
t1c in (select t3c from t3 where t1a = t3a) group by t1a, t1c;
-- TC 01.06
select t1a, count(distinct(t1b)) from t1 where t1c in (select t2c from t2 where t1a = t2a)
group by t1a, t1c having t1a = "t1b";

-- GROUP BY in subquery
-- TC 01.07
select * from t1 where t1b in (select max(t2b) from t2 group by t2a);
-- TC 01.08
select * from (select t2a, t2b from t2 where t2a in (select t1a from t1 where t1b = t2b) group by t2a, t2b) t2;
-- TC 01.09
select count(distinct(*)) from t1 where t1b in (select min(t2b) from t2 where t1a = t2a and t1c = t2c group by t2a);
-- TC 01.10
select t1a, t1b from t1 where t1c in (select max(t2c) from t2 where t1a = t2a group by t2a, t2c having t2c > 8);
-- TC 01.11
select t1a, t1b from t1 where t1c in (select t2c from t2 where t2a in
( select min(t3a) from t3 where t3a = t2a group by t3b) group by t2c);
-- TC 01.12
select * from t1 where t1a in
(select min(t2a) from t2 where t2a = t2a and t2c >= 1 group by t2c having t2c in
(select t3c from t3 group by t3c, t3b having t2b > 6 and t3b > t2b ));
-- TC 01.13
select * from (select * from t2 where t2a in (select t1a from t1 where t1b = t2b)) t2 where t2a in
(select t2a from t2 where t2a = t2a and t2c > 1 group by t2a having t2c > 8);

-- GROUP BY in both
-- TC 01.14
select t1a, min(t1b) from t1 where t1c in (select min(t2c) from t2 where t2b = t1b group by t2a) group by t1a;
-- TC 01.15
select t1a, min(t1b) from t1 where t1c in (select min(t2c) from t2 where t2b in (select min(t3b) from t3
where t2a = t3a group by t3a) group by t2c) group by t1a, t1d;
-- TC 01.16
select t1a, min(t1b) from t1 where t1c in (select min(t2c) from t2 where t2b = t1b group by t2a) and
t1d in (select t3d from t3 where t1c = t3c group by t3d) group by t1a;
-- TC 01.17
select t1a, min(t1b) from t1 where t1c in (select min(t2c) from t2 where t2b = t1b group by t2a) or
t1d in (select t3d from t3 where t1c = t3c group by t3d) group by t1a;
-- TC 01.18
select t1a, min(t1b) from t1 where t1c in (select min(t2c) from t2 where t2b = t1b group by t2a having t2a > t1a) or
t1d in (select t3d from t3 where t1c = t3c group by t3d having t3d = t1d) group by t1a having min(t1b) is NOT NULL;

-- Clean Up
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
USE default;
DROP DATABASE indb;