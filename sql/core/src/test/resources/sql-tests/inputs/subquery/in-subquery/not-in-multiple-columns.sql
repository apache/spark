-- This file contains test cases for NOT IN subquery with multiple columns.

-- The data sets are populated as follows:
-- 1) When T1.A1 = T2.A2
--    1.1) T1.B1 = T2.B2
--    1.2) T1.B1 = T2.B2 returns false
--    1.3) T1.B1 is null
--    1.4) T2.B2 is null
-- 2) When T1.A1 = T2.A2 returns false
-- 3) When T1.A1 is null
-- 4) When T1.A2 is null

-- T1.A1  T1.B1     T2.A2  T2.B2
-- -----  -----     -----  -----
--     1      1         1      1    (1.1)
--     1      3                     (1.2)
--     1   null         1   null    (1.3 & 1.4)
--
--     2      1         1      1    (2)
--  null      1                     (3)
--                   null      3    (4)

create temporary view t1 as select * from values
  (1, 1), (2, 1), (null, 1),
  (1, 3), (null, 3),
  (1, null), (null, 2)
as t1(a1, b1);

create temporary view t2 as select * from values
  (1, 1),
  (null, 3),
  (1, null)
as t2(a2, b2);

-- multiple columns in NOT IN
-- TC 01.01
select a1,b1
from   t1
where  (a1,b1) not in (select a2,b2
                       from   t2);

-- multiple columns with expressions in NOT IN
-- TC 01.02
select a1,b1
from   t1
where  (a1-1,b1) not in (select a2,b2
                         from   t2);

-- multiple columns with expressions in NOT IN
-- TC 01.02
select a1,b1
from   t1
where  (a1,b1) not in (select a2+1,b2
                       from   t2);

