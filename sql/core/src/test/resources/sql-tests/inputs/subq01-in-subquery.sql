-- A test suite for IN predicate subquery
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
 ('t1b', 8, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1a', 16, 12, 21, 15, 20, 20.00, timestamp(date("2014-04-04")), date("2014-04-04")),
 ('t1a', 16, 12, 10, 15, 20, 20.00, timestamp(date("2014-04-04")), date("2014-04-04")),
 ('t1c', 8, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1d', null, 16, 22, 17, 25, 26.00, timestamp(date("2014-05-04")), null),
 ('t1d', null, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), null),
 ('t1e', 10, null, 25, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1e', 10, null, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1d', 10, null, 12, 17, 25, 26.00, timestamp(date("2015-05-04")), date("2015-04-05")),
 ('t1a', 6, 8, 10, 15, 20, 20.00, timestamp(date("2014-04-04")), date("2014-04-04")),
 ('t1e', 10, null, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05"));

INSERT INTO t2 VALUES
 ('t2a', 6, 12, 10, 15, 20, 20.00, timestamp(date("2014-04-04")), date("2014-04-04")),
 ('t1b', 10, 12, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1b', 8, 16, 119, 17, 25, 26.00, timestamp(date("2015-05-04")), date("2014-04-05")),
 ('t1c', 12, 16, 219, 17, 25, 26.00, timestamp(date("2016-05-04")), date("2014-04-05")),
 ('t1b', null, 16, 319, 17, 25, 26.00, timestamp(date("2017-05-04")), null),
 ('t2e', 8, null, 419, 17, 25, 26.00, timestamp(date("2014-06-04")), date("2014-04-05")),
 ('t1f', 19, null, 519, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1b', 10, 12, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1b', 8, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1c', 12, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1e', 8, null, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1f', 19, null, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1b', null, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), null);

INSERT INTO t3 VALUES
 ('t3a', 6, 12, 110, 15, 20, 20.00, timestamp(date("2014-04-04")), date("2014-04-04")),
 ('t3a', 6, 12, 10, 15, 20, 20.00, timestamp(date("2014-04-04")), date("2014-04-04")),
 ('t1b', 10, 12, 219, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1b', 10, 12, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1b', 8, 16, 319, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1b', 8, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t3c', 17, 16, 519, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t3c', 17, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t1b', null, 16, 419, 17, 25, 26.00, timestamp(date("2014-05-04")), null),
 ('t1b', null, 16, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), null),
 ('t3b', 8, null, 719, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05")),
 ('t3b', 8, null, 19, 17, 25, 26.00, timestamp(date("2014-05-04")), date("2014-04-05"));

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
select t1a, t1b from t1 where t1c in (select t2b from t2 where t2a in
                      (select t3a from t3 where t2c = t3c));
-- TC 01.06
select t1a, t1b from t1 where t1c in (select t2b from t2 where t2a in
                      (select t3a from t3 where t2c = t3c and t2b is not NULL));
-- simple select for NOT IN
-- TC 01.07
select * from t1 where t1a not in (select t2a from t2);
-- TC 01.08
select * from t1 where t1d not in (select t2d from t2 where t2h > t1h or t2i > t1i);
-- TC 01.09
select * from t1 where t1b not in (select t2b from t2 where t1a < t2a and t2b > 6);

-- GROUP BY in parent side
-- TC 02.01
select t1a, avg(t1b) from t1 where t1a in (select t2a from t2) group by t1a;
-- TC 02.02
select t1a, max(t1b) from t1 where t1b in (select t2b from t2 where t1a = t2a) group by t1a, t1d;
-- TC 02.03
select t1a, sum(t1b) from t1 where t1c in (select t2c from t2 where t1a = t2a) group by t1a, t1c;
-- TC 02.04
select t1a, sum(distinct(t1b)) from t1 where t1c in (select t2c from t2 where t1a = t2a) or
t1c in (select t3c from t3 where t1a = t3a) group by t1a, t1c;
-- TC 02.05
select t1a, count(distinct(t1b)) from t1 where t1c in (select t2c from t2 where t1a = t2a)
group by t1a, t1c having t1a = "t1b";

-- GROUP BY in subquery
-- TC 02.06
select * from t1 where t1b in (select max(t2b) from t2 group by t2a);
-- TC 02.07
select * from (select * from t2 where t2a in (select t1a from t1 where t1b = t2b)) t2;
-- TC 02.08
select count(distinct(*)) from t1 where t1b in (select min(t2b) from t2 where t1a = t2a and t1c = t2c group by t2a);
-- TC 02.09
select t1a, t1b from t1 where t1c in (select max(t2c) from t2 where t1a = t2a group by t2a, t2c having t2c > 8);
-- TC 02.10
select t1a, t1b from t1 where t1c in (select t2c from t2 where t2a in
( select min(t3a) from t3 where t3a = t2a group by t3b));
-- TC 02.11
select * from t1 where t1a in
(select min(t2a) from t2 where t2a = t2a and t2c >= 1 group by t2c having t2c in
(select t3c from t3 having t2b > 6 and t3b > t2b));
-- TC 02.12
select * from (select * from t2 where t2a in (select t1a from t1 where t1b = t2b)) t2 where t2a in
(select t2a from t2 where t2a = t2a and t2c > 1 group by t2a having t2c > 8);

-- GROUP BY with NOT IN
-- TC 02.13
select t1a, avg(t1b) from t1 where t1a not in (select t2a from t2) group by t1a;
-- TC 02.14
select t1a, sum(distinct(t1b)) from t1 where t1d not in (select t2d from t2 where t1h < t2h) group by t1a;
-- TC 02.15
select * from t1 where t1b not in (select max(t2b) from t2 where t2i not in
 (select t1i from t1 where t1d > t2d) group by t2a);
-- TC 02.16
select count(*) from (select * from t2 where t2a not in (select t3a from t3 where t3h != t2h)) t2
where t2b not in (select min(t2b) from t2 where t2b = t2b group by t2c);
-- TC 02.17
select t1a, t1b from t1 where t1c not in (select max(t2b) from t2 where t1a = t2a group by t2a);
-- TC 02.18
select t1a, t1b from t1 where t1c in (select t2b from t2 where t2a not in
( select min(t3a) from t3 where t3a = t2a group by t3b));

-- ORDER BY in the parent side
-- TC 03.01
select * from t1 where t1a in (select t2a from t2) order by t1a;
-- TC 03.02
select t1a from t1 where t1b in (select t2b from t2 where t1a = t2a) order by t1b desc;
-- TC 03.03
select t1a, t1b from t1 where t1c in (select t2c from t2 where t1a = t2a) order by 1;
-- TC 03.04
select count(distinct(t1a)) from t1 where t1b in (select t2b from t2 where t1a = t2a) order by count(distinct(t1a));

-- ORDER BY in subquery
-- TC 03.05
select * from t1 where t1a in (select t2a from t2 order by t2a);
-- TC 03.06
select * from t1 where t1b in (select min(t2b) from t2 where t1a = t2a order by min(t2b));
-- TC 03.07
select t1a, t1b, t1h from t1 where t1c in (select t2c from t2 where t1a = t2a order by t2b desc) or
                                   t1h in (select t2h from t2 where t1h > t2h) order by t1h asc;

-- ORDER BY with NOT IN
-- TC 03.08
select * from t1 where t1a not in (select t2a from t2) order by t1a;
-- TC 03.09
select t1a, t1b from t1 where t1a not in (select t2a from t2 where t1a = t2a) order by t1a;
-- TC 03.10
select * from t1 where t1a not in (select t2a from t2 order by t2a desc) and
                       t1c in (select t2c from t2 order by t2b asc) order by t1c desc;

-- GROUP BY and ORDER BY in subquery
-- TC 04.01
select * from t1 where t1b in (select min(t2b) from t2 group by t2a order by t2a desc);
-- TC 04.02
select count(distinct(*)) from t1 where t1a in (select min(t2a) from t2 group by t2c order by t2c desc);
-- TC 04.03
select t1a, count(distinct(t1b)) from t1 where t1b in (select min(t2b)
from t2 where t1a = t2a group by t2a order by t2a desc) group by t1a, t1h;
-- TC 04.04
select t1a, t1b from t1 where t1b in (select min(t2b) from t2
where t1a = t2a group by t2d order by t2d desc);

-- GROUP BY and ORDER BY with NOT IN
-- TC 04.05
select * from t1 where t1b not in (select min(t2b) from t2 group by t2a order by t2a);
-- TC 04.06
select count(*) from t1 where t1b not in (select min(t2b) from t2 group by t2c order by t2c desc) group by t1a;
-- TC 04.07
select t1a, sum(distinct(t1b)) from t1 where t1b not in (select min(t2b) from t2
where t1a = t2a group by t2c order by t2c) group by t1a;
-- TC 04.08
select count(distinct(t1a)), t1b from t1 where t1b not in (select min(t2b) from t2
where t1a = t2a group by t2d order by t2d desc) group by t1a, t1b order by t1b desc;

-- LIMIT in subquery
-- TC 05.01
select * from t1 where t1a in (select t2a from t2 limit 2);
-- TC 05.02
select * from t1 where t1c in (select t2c from t2 where t2b >=8 limit 2) limit 4;
-- TC 05.03
select count(distinct(t1a)), t1b from t1 where t1d in (select t2d from t2 order by t2c)
group by t1b order by t1b limit 1;

-- LIMIT with NOT IN
-- TC 05.04
select * from t1 where t1a not in (select t2a from t2 limit 2);
-- TC 05.05
select * from t1 where t1b not in (select t2b from t2 where t2b > 6 limit 2);
-- TC 05.06
select count(distinct(t1a)), t1b from t1 where t1d not in
(select t2d from t2 order by t2b) group by t1b limit 1;

-- HAVING clause in subquery
-- TC 06.01
select * from t1 where t1a in (select t2a from t2 having t2c > 10);
-- TC 06.02
select * from t1 where t1b in (select min(t2b) from t2 where t1a = t2a group by t2b having t2b > 1);
-- TC 06.03
select count(distinct(t1a)), t1b from t1 where t1c in (select t2c from t2 where t1a = t2a having t2c > 10)
group by t1b having t1b >= 8;
-- TC 06.04
select t1a, max(t1b) from t1 where t1b > 0 group by t1a having t1a in
(select t2a from t2 where t2b in (select t3b from t3 where t2c = t3c));

-- HAVING clause with NOT IN
-- TC 06.05
select * from t1 where t1a not in (select t2a from t2 having t2c > 10);
-- TC 06.06
select t1a, t1b from t1 where t1d not in (select t2d from t2 where t1a = t2a having t2c > 8);
-- TC 06.07
select t1a, t1b from t1 where t1c not in (select t2c from t2 where t1a = t2a having t2c not in
(select t3c from t3)) group by t1a, t1b limit 2;
-- TC 06.10
select t1a, max(t1b) from t1 where t1b > 0 group by t1a having t1a not in (select t2a from t2 where t2b > 3);

-- different JOIN in parent side
-- TC 07.01
select * from t1 natural join t3 where t1a in (select t2a from t2 ) and t1b = t3b order by t1a, t1b;
-- TC 07.02
select count(distinct(t1a)), t1b, t3a, t3b, t3c from t1 natural left join t3 where t1a
in (select t2a from t2 ) and t1b > t3b group by t1a, t1b, t3a, t3b, t3c order by t1a desc limit 2;
-- TC 07.03
select count(distinct(*)) from t1 natural right join t3
where t1a in (select t2a from t2 where t1b = t2b) and t1b in (select t2b from t2 where t1c > t2c) and t1a = t3a
group by t1a order by t1a;
-- TC 07.04
select t1a, t1b, t1c, t3a, t3b, t3c from t1 full outer join t3
where t1a in (select t2a from t2 ) and t1b != t3b order by t1a;
-- TC 07.05
select count(distinct(t1a)), t1b from t1 full outer join t3 where t1a in (select t2a from t2 where t2h > t3h)
and t3a in (select t2a from t2 where t2c > t3c) and t1h >= t3h group by t1a, t1b order by t1a;
-- TC 07.06
select count(*) from t1 left semi join t3 where t1a in (select t2a from t2 ) group by t1a order by t1a;
-- TC 07.07
select count(distinct(t1a)), t1b from t1 left anti join t2 on t1a > t2a where t1b in (select t2b from t2)
or t1a in (select t2a from t2) group by t1b having t1b > 1;

-- different JOIN in the subquery
-- TC 07.08
select * from t1 where t1a in (select t2a from t2 left join t3 where t2b = t3b);
-- TC 07.09
select t1a, t1b from t1 where t1a in (select t2a from t2 full outer join t1 where t2b > t1b) limit 2;
-- TC 07.09
select count(distinct(t1a)), t1b from t1 where t1a in (select t2a from t2 cross join t1 where t2b <> t1b) and
t1h in (select t2h from t2 right join t3 where t2b = t3b) group by t1b;
-- TC 07.10
select count(distinct(t1a)), t1b from t1 where t1a in (select t2a from t2 right join t1 where t2b <> t1b) or
                              t1b in (select t2b from t2 left join t1 where t2c = t1c) group by t1b;

-- JOIN in the parent and subquery
-- TC 07.11
select * from t1 full outer join t2 where t1a in
(select t2a from t2 left join t3 where t2b = t3b) and t1c = t2c limit 10;
-- TC 07.12
select count(distinct(t1a)), t1b from t1 inner join t2
where t1a in (select t2a from t2 join t3 where t2b > t3b) and
t1c in (select min(t3c) from t3 left outer join t2 group by t3a having t3a = t2a) and t1a = t2a
group by t1b order by t1b desc;
-- TC 07.13
select t1a, t1b, t1c, t2a, t2b, t2c from t1 right join t2
where t1a in (select t2a from t2 full outer join t3 where t2b > t3b) and
t1b in ( select t3b from t3 left semi join t1 where t3c = t1c) and t1h > t2h;
-- TC 07.15
select t1a, t1b, t1c, t2a, t2b, t2c from t1 full join t2
where t1a in (select t2a from t2 left anti join t3 on t2b < t3b where t2c in (select t1c from t1 where t1a = t2a))
and t1a = t2a;
-- TC 07.16
select * from (select * from t3 left outer join t2 where t3a = t2a and t2a in
(select t1a from t1 where t2b = t1b)) t1 natural full outer join t2
where t1.t2a in (select t2a from t2 left anti join t3 on t2b < t3b) and t1.t2c = t2c order by t2a, t3a desc limit 5;

-- JOIN with NOT IN
-- TC 07.17
select t1a, t1b, t1c, t3a, t3b, t3c from t1 natural join t3 where t1a not in (select t2a from t2 ) and t1b = t3b;
-- TC 07.18
select t1a, t1b, t1c, t3a, t3b, t3c from t1 full outer join t3
where t1a not in (select t2a from t2 where t2c not in (select t1c from t1 where t1a = t2a)) and t1b != t3b order by t1a;
-- TC 07.19
select count(*) from t1 left semi join t3 where t1a not in (select t2a from t2 left outer join t3 where t2a = t3a)
group by t1a order by t1a;
-- TC 07.20
select * from t1 where t1a not in (select t2a from t2 left join t3 where t2b = t3b) and t1d not in (select t2d from t2);
-- TC 07.21
select count(distinct(t1a)), t1b, t1c, t1d from t1 where t1a not in (select t2a from t2 cross join t1 where t2b <> t1b)
group by t1b, t1c, t1d having t1d not in (select t2d from t2 where t1d = t2d);
-- TC 07.22
select sum(distinct(t1a)), t1b, t1c, t1d from t1 where t1a not in (select t2a from t2 left semi join t1)
group by t1b, t1c, t1d having t1b < sum(t1c);
-- TC 07.23
select * from t1 full outer join t2 where t1a not in (select t2a from t2 left join t3 where t2b = t3b) and t1c = t2c;
-- TC 07.24
select * from t1 full outer join t2 where t1a not in (select t2a from t2 inner join t3 where t2b = t3b) and t1c = t2c;

-- UNION, UNION ALL, UNION DISTINCT, INTERSECT and EXCEPT in the parent
-- TC 08.01
select t2a from (select t2a from t2 where t2a in (select t1a from t1)
       union all select t2a from t2 where t2a in (select t1a from t1)) as t3;
-- TC 08.02
select t2a from (select t2a from t2 where t2a in (select t1a from t1)
       union select t2a from t2 where t2a in (select t3a from t3)) as t3;
-- TC 08.03
select t2a from (select t2a from t2 where t2a in (select t1a from t1 where t1b = t2b)
       union all select t2a from t2 where t2a in (select t1a from t1 where t2c = t1c)) as t3;
-- TC 08.04
select t2a from (select t2a from t2 where t2a in (select t1a from t1 where t1b = t2b)
       union select t2a from t2 where t2a in (select t1a from t1 where t2c = t1c)) as t3;
-- TC 08.05
select t2a from (select t2a from t2 where t2a in (select t1a from t1 where t1b = t2b)
       union distinct select t2a from t2 where t2a in (select t1a from t1 where t2c = t1c)) as t3;
-- TC 08.06
select t2a from (select t2a from t2 where t2a in (select t1a from t1 where t1b = t2b)
       intersect select t2a from t2 where t2a in (select t1a from t1 where t2c = t1c)) as t3;
-- TC 08.07
select t2a from (select t2a from t2 where t2a in (select t1a from t1 where t1b != t2b)
       except select t2a from t2 where t2a in (select t3a from t3 where t2c = t3c)) as t3;

-- UNION, UNION ALL, UNION DISTINCT, INTERSECT and EXCEPT in the subquery
-- TC 08.08
select * from t1 where t1a in (select t3a from (select t2a t3a from t2 union all select t2a t3a from t2) as t3);
-- TC 08.09
select * from t1 where t1b in (
select t2b from (select t2b from t2 where t2b > 6 union select t2b from t2 where t2b > 6) as t3);
-- TC 08.10
select t1a, t1b, t1c from t1 where t1b in (
select t2b from (select t2b from t2 where t2b > 6 union distinct select t1b from t1 where t1b > 6) as t3);
-- TC 08.11
select t1a, t1b, t1c from t1 where t1b in (
select t2b from (select t2b from t2 where t2b > 6 intersect select t1b from t1 where t1b > 6) as t3 where t2b = t1b);
-- TC 08.12
select t1a, t1b, t1c from t1 where t1h in (
select t2h from (select t2h from t2 except select t3h from t3 ) as t3);

-- UNION, UNION ALL, UNION DISTINCT, INTERSECT and EXCEPT in the parent and subquery
-- TC 08.13
select t1a, t1b, t1c from t1 where t1a in (select t3a from (select t2a t3a from t2 union all select t2a t3a from t2)
 as t3)
union all
select t1a, t1b, t1c from t1 where t1a in (select t3a from (select t2a t3a from t2 union select t2a t3a from t2)
 as t3);
-- TC 08.14
select t1a, t1b, t1c from t1 where t1b in (
select t2b from (select t2b from t2 where t2b > 6 intersect select t1b from t1 where t1b > 6) as t3)
union distinct
select t1a, t1b, t1c from t1 where t1b in (
select t2b from (select t2b from t2 where t2b > 6 except select t1b from t1 where t1b > 6) as t3 where t2b = t1b);
-- TC 08.15
select count(distinct t2b) from
(select t2a, t2b from (select t2a, t2b from t2 where t2h in (select t1h from t1 where t1a = t2a)
         union distinct select t1a, t1b from t1 where t1h in (select t3h from t3 union select t1h from t1)) t4
         where t4.t2b in (select min(t3b) from t3 where t4.t2a = t3a));

-- UNION, UNION ALL, UNION DISTINCT, INTERSECT and EXCEPT for NOT IN
-- TC 08.16
select t2a from (select t2a from t2 where t2a not in (select t1a from t1 union select t3a from t3)
       union all select t2a from t2 where t2a not in (select t1a from t1 intersect select t2a from t2)) as t3
       where t3.t2a not in (select t1a from t1 intersect select t2a from t2);
-- TC 08.17
select t1a, t1b, t1c from t1 where t1b not in (
select t2b from (select t2b from t2 where t2b not in (select t1b from t1) union
                 select t1b from t1 where t1b not in (select t3b from t3) union distinct
                 select t3b from t3 where t3b not in (select t2b from t2)) as t3 where t2b = t1b);
-- TC 08.18
select distinct(t1a), t1b, t1c from t1 where t1b not in (
select t2b from (select t2b from t2 where t2b > 6 union distinct select t1b from t1 where t1b > 6) as t3)
union
select distinct(t1a), t1b, t1c from t1 where t1b not in (
select t2b from (select t2b from t2 where t2b > 6 intersect select t1b from t1 where t1b > 6) as t3 where t2b = t1b)
group by t1a, t1b, t1c;

-- outside CTE
-- TC 09.01
with cte1 as (select t1a, t1b from t1) select * from t1 where t1b in
(select cte1.t1b from cte1 where cte1.t1b > 0);
-- TC 09.02
with cte1 as (select t1a, t1b from t1) select t1a, t1b from t1 where t1b in
(select cte1.t1b from cte1 where cte1.t1b > 0 union select cte1.t1b from cte1 where cte1.t1b > 5);
-- TC 09.03
with cte1 as (select t1a, t1b from t1) select * from
(select cte1.t1b from cte1 where cte1.t1b > 0 union select cte1.t1b from cte1 where cte1.t1b > 5) s where t1b > 6;
-- TC 09.04
with cte1 as (select t1a, t1b from t1) select distinct(*) from
(select cte1.t1b from cte1 cross join cte1 cte2 on cte1.t1b > 5) s where t1b > 6;

-- CTE inside and outside
-- TC 09.05
with cte1 as (select t1a, t1b from t1 where t1b in (select t2b from t2 where t1c = t2c)) select distinct(*) from
(select cte1.t1b from cte1 cross join cte1 cte2 on cte1.t1b > 5 and cte1.t1a = cte2.t1a) s where t1b in (8);
-- TC 09.06
with cte1 as (select t1a, t1b, t1h from t1 where t1a in (select t2a from t2 where t1b < t2b))
select count(distinct t1a), t1b from
(select cte1.t1a, cte1.t1b from cte1 cross join cte1 cte2 on cte1.t1h >= cte2.t1h) s where t1b in (select t1b from t1)
group by t1b;
-- TC 09.07
with cte1 as (select t1a, t1b from t1 where t1b in (select t2b from t2 where t1c = t2c)) select distinct(*) from
(select cte1.t1b from cte1 left semi join cte1 cte2 on cte1.t1b > 5 and cte1.t1a != cte2.t1a) s;
-- TC 09.08
with cte1 as (select t1a, t1b from t1 where t1b in (select t2b from t2 where t1c = t2c))
select count(distinct(s.t1a)), s.t1b from
(select cte1.t1a, cte1.t1b from cte1 right outer join cte1 cte2 on cte1.t1a = cte2.t1a ) s
where t1b in (8) group by s.t1b;
-- TC 09.09
with cte1 as (select t1a, t1b from t1 where t1b in (select t2b from t2 where t1c = t2c)) select distinct(*) from
(select cte1.t1b from cte1 left outer join cte1 cte2 on cte1.t1b = cte2.t1b) s
where s.t1b in (select t1b from t1 left semi join cte1 on t1.t1a = cte1.t1a);

-- CTE with NOT IN
-- TC 09.10
with cte1 as (select t1a, t1b from t1) select * from t1 where t1b not in (select cte1.t1b from cte1 where cte1.t1b < 0);
-- TC 09.11
with cte1 as (select t1a, t1b from t1 where t1a not in (select t2a from t2)) select distinct(*) from
(select cte1.t1b from cte1 cross join cte1 cte2) s;
-- TC 09.12
with cte1 as (select t1a, t1b, t1c, t1h from t1 where t1d not in (select t2d from t2)) select * from
(select cte1.t1b, cte1.t1c, cte1.t1h from cte1 full outer join cte1 cte2 where cte1.t1h = cte2.t1h) s where s.t1h
not in (select t3h from t3);

-- multiple columns in the IN subquery
-- TC 10.01
select * from t1 where (t1a, t1h) not in
(select t2a, t2h from t2 where t2a = t1a order by t2a);
-- TC 10.02
select * from t1 where (t1b, t1d) in (select t2b, t2d from t2 where t2h in (select t3h from t3 where t2b > t3b));
-- TC 10.03
select * from t1 where (t1b, t1d) not in (select t2b, t2d from t2 where t2h in (select t3h from t3 where t2b > t3b));
-- TC 10.05
select t2a from (select t2a from t2 where (t2a, t2b) in (select t1a, t1b from t1) union all
                 select t2a from t2 where (t2a, t2b) in (select t1a, t1b from t1) union distinct
                 select t2a from t2 where (t2a, t2b) in (select t3a, t3b from t3)) as t4;
-- TC 10.06
select * from (select * from t3 left outer join t2 where t3a = t2a and (t2a, t2b) in
(select t1a, t1b from t1 where t2b = t1b)) t1 natural full outer join t2
where (t1.t2a, t1.t2b) not in (select t2a, t2b from t2 left anti join t3 on t2a = t3a) order by t2a, t3a desc limit 5;
-- TC 10.07
with cte1 as (select t1a, t1b from t1 where (t1b, t1d) in (select t2b, t2d from t2 where t1c = t2c))
select distinct(*) from (select cte1.t1b from cte1 cross join cte1 cte2 on cte1.t1b > 5) s where t1b in (8);

-- Clean Up
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
USE default;
DROP DATABASE indb;