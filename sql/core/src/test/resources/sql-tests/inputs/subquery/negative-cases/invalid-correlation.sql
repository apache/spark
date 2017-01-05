-- The test file contains negative test cases
-- of invalid queries where error messages are expected.

create temporary view t1 as select * from values
  (1, 2, 3)
as t1(t1a, t1b, t1c);

create temporary view t2 as select * from values
  (1, 0, 1)
as t2(t2a, t2b, t2c);

create temporary view t3 as select * from values
  (3, 1, 2)
as t3(t3a, t3b, t3c);

-- TC 01.01
-- The column t2b in the SELECT of the subquery is invalid
-- because it is neither an aggregate function nor a GROUP BY column.
select t1a, t2b
from   t1, t2
where  t1b = t2c
and    t2b = (select max(avg)
              from   (select   t2b, avg(t2b) avg
                      from     t2
                      where    t2a = t1.t1b
                     )
             )
;

-- TC 01.02
-- Invalid due to the column t2b not part of the output from table t2.
select *
from   t1
where  t1a in (select   min(t2a)
               from     t2
               group by t2c
               having   t2c in (select   max(t3c)
                                from     t3
                                group by t3b
                                having   t3b > t2b ))
;

-- TC 01.03
-- The column t2c in the predicate t2c > 8 must be mapped to the t2 in its subquery scope.
-- But t2c is not part of the output of the subquery hence this is an invalid query.
select *
from   (select *
        from   t2
        where  t2a in (select t1a
                       from   t1
                       where  t1b = t2b)) t2
where  t2a in (select   t2a
               from     t2
               where    t2a = t2a
               and      t2c > 1
               group by t2a
               having   t2c > 8)
;
