create table smallTbl1(key string, value string);
insert overwrite table smallTbl1 select * from src where key < 10;

create table smallTbl2(key string, value string);
insert overwrite table smallTbl2 select * from src where key < 10;

create table smallTbl3(key string, value string);
insert overwrite table smallTbl3 select * from src where key < 10;

create table smallTbl4(key string, value string);
insert overwrite table smallTbl4 select * from src where key < 10;

create table bigTbl(key string, value string);
insert overwrite table bigTbl
select * from
(
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
   union all
 select * from src
) subq;

set hive.auto.convert.join=true;

explain
select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1 
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN                                                                  
smallTbl2 on (firstjoin.value1 = smallTbl2.value);

select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1 
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN                                                                  
smallTbl2 on (firstjoin.value1 = smallTbl2.value);

set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

-- Now run a query with two-way join, which should be converted into a
-- map-join followed by groupby - two MR jobs overall 
explain
select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1 
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN                                                                  
smallTbl2 on (firstjoin.value1 = smallTbl2.value);

select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1 
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN
smallTbl2 on (firstjoin.value1 = smallTbl2.value);

-- Now run a query with two-way join, which should first be converted into a
-- map-join followed by groupby and then finally into a single MR job.

explain
select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN
smallTbl2 on (firstjoin.value1 = smallTbl2.value)
group by smallTbl2.key;

select count(*) FROM
(select bigTbl.key as key, bigTbl.value as value1,
 bigTbl.value as value2 FROM bigTbl JOIN smallTbl1
 on (bigTbl.key = smallTbl1.key)
) firstjoin
JOIN
smallTbl2 on (firstjoin.value1 = smallTbl2.value)
group by smallTbl2.key;

drop table bigTbl;

create table bigTbl(key1 string, key2 string, value string);
insert overwrite table bigTbl
select * from
(
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
   union all
 select key as key1, key as key2, value from src
) subq;

set hive.auto.convert.join.noconditionaltask=false;
-- First disable noconditionaltask
EXPLAIN
SELECT SUM(HASH(join3.key1)),
       SUM(HASH(join3.key2)),
       SUM(HASH(join3.key3)),
       SUM(HASH(join3.key4)),
       SUM(HASH(join3.key5)),
       SUM(HASH(smallTbl4.key)),
       SUM(HASH(join3.value1)),
       SUM(HASH(join3.value2))
FROM (SELECT join2.key1 as key1,
             join2.key2 as key2,
             join2.key3 as key3,
             join2.key4 as key4,
             smallTbl3.key as key5,
             join2.value1 as value1,
             join2.value2 as value2
      FROM (SELECT join1.key1 as key1,
                   join1.key2 as key2,
                   join1.key3 as key3,
                   smallTbl2.key as key4,
                   join1.value1 as value1,
                   join1.value2 as value2
            FROM (SELECT bigTbl.key1 as key1,
                         bigTbl.key2 as key2,
                         smallTbl1.key as key3,
                         bigTbl.value as value1,
                         bigTbl.value as value2
                  FROM bigTbl
                  JOIN smallTbl1 ON (bigTbl.key1 = smallTbl1.key)) join1
            JOIN smallTbl2 ON (join1.value1 = smallTbl2.value)) join2
      JOIN smallTbl3 ON (join2.key2 = smallTbl3.key)) join3
JOIN smallTbl4 ON (join3.key3 = smallTbl4.key);

SELECT SUM(HASH(join3.key1)),
       SUM(HASH(join3.key2)),
       SUM(HASH(join3.key3)),
       SUM(HASH(join3.key4)),
       SUM(HASH(join3.key5)),
       SUM(HASH(smallTbl4.key)),
       SUM(HASH(join3.value1)),
       SUM(HASH(join3.value2))
FROM (SELECT join2.key1 as key1,
             join2.key2 as key2,
             join2.key3 as key3,
             join2.key4 as key4,
             smallTbl3.key as key5,
             join2.value1 as value1,
             join2.value2 as value2
      FROM (SELECT join1.key1 as key1,
                   join1.key2 as key2,
                   join1.key3 as key3,
                   smallTbl2.key as key4,
                   join1.value1 as value1,
                   join1.value2 as value2
            FROM (SELECT bigTbl.key1 as key1,
                         bigTbl.key2 as key2,
                         smallTbl1.key as key3,
                         bigTbl.value as value1,
                         bigTbl.value as value2
                  FROM bigTbl
                  JOIN smallTbl1 ON (bigTbl.key1 = smallTbl1.key)) join1
            JOIN smallTbl2 ON (join1.value1 = smallTbl2.value)) join2
      JOIN smallTbl3 ON (join2.key2 = smallTbl3.key)) join3
JOIN smallTbl4 ON (join3.key3 = smallTbl4.key);

set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
-- Enable noconditionaltask and set the size of hive.auto.convert.join.noconditionaltask.size
-- to 10000, which is large enough to fit all four small tables (smallTbl1 to smallTbl4).
-- We will use a single MR job to evaluate this query.
EXPLAIN
SELECT SUM(HASH(join3.key1)),
       SUM(HASH(join3.key2)),
       SUM(HASH(join3.key3)),
       SUM(HASH(join3.key4)),
       SUM(HASH(join3.key5)),
       SUM(HASH(smallTbl4.key)),
       SUM(HASH(join3.value1)),
       SUM(HASH(join3.value2))
FROM (SELECT join2.key1 as key1,
             join2.key2 as key2,
             join2.key3 as key3,
             join2.key4 as key4,
             smallTbl3.key as key5,
             join2.value1 as value1,
             join2.value2 as value2
      FROM (SELECT join1.key1 as key1,
                   join1.key2 as key2,
                   join1.key3 as key3,
                   smallTbl2.key as key4,
                   join1.value1 as value1,
                   join1.value2 as value2
            FROM (SELECT bigTbl.key1 as key1,
                         bigTbl.key2 as key2,
                         smallTbl1.key as key3,
                         bigTbl.value as value1,
                         bigTbl.value as value2
                  FROM bigTbl
                  JOIN smallTbl1 ON (bigTbl.key1 = smallTbl1.key)) join1
            JOIN smallTbl2 ON (join1.value1 = smallTbl2.value)) join2
      JOIN smallTbl3 ON (join2.key2 = smallTbl3.key)) join3
JOIN smallTbl4 ON (join3.key3 = smallTbl4.key);

SELECT SUM(HASH(join3.key1)),
       SUM(HASH(join3.key2)),
       SUM(HASH(join3.key3)),
       SUM(HASH(join3.key4)),
       SUM(HASH(join3.key5)),
       SUM(HASH(smallTbl4.key)),
       SUM(HASH(join3.value1)),
       SUM(HASH(join3.value2))
FROM (SELECT join2.key1 as key1,
             join2.key2 as key2,
             join2.key3 as key3,
             join2.key4 as key4,
             smallTbl3.key as key5,
             join2.value1 as value1,
             join2.value2 as value2
      FROM (SELECT join1.key1 as key1,
                   join1.key2 as key2,
                   join1.key3 as key3,
                   smallTbl2.key as key4,
                   join1.value1 as value1,
                   join1.value2 as value2
            FROM (SELECT bigTbl.key1 as key1,
                         bigTbl.key2 as key2,
                         smallTbl1.key as key3,
                         bigTbl.value as value1,
                         bigTbl.value as value2
                  FROM bigTbl
                  JOIN smallTbl1 ON (bigTbl.key1 = smallTbl1.key)) join1
            JOIN smallTbl2 ON (join1.value1 = smallTbl2.value)) join2
      JOIN smallTbl3 ON (join2.key2 = smallTbl3.key)) join3
JOIN smallTbl4 ON (join3.key3 = smallTbl4.key);
 
set hive.auto.convert.join.noconditionaltask.size=200;
-- Enable noconditionaltask and set the size of hive.auto.convert.join.noconditionaltask.size
-- to 200, which is large enough to fit two small tables. We will have two jobs to evaluate this
-- query. The first job is a Map-only job to evaluate join1 and join2.
-- The second job will evaluate the rest of this query.
EXPLAIN
SELECT SUM(HASH(join3.key1)),
       SUM(HASH(join3.key2)),
       SUM(HASH(join3.key3)),
       SUM(HASH(join3.key4)),
       SUM(HASH(join3.key5)),
       SUM(HASH(smallTbl4.key)),
       SUM(HASH(join3.value1)),
       SUM(HASH(join3.value2))
FROM (SELECT join2.key1 as key1,
             join2.key2 as key2,
             join2.key3 as key3,
             join2.key4 as key4,
             smallTbl3.key as key5,
             join2.value1 as value1,
             join2.value2 as value2
      FROM (SELECT join1.key1 as key1,
                   join1.key2 as key2,
                   join1.key3 as key3,
                   smallTbl2.key as key4,
                   join1.value1 as value1,
                   join1.value2 as value2
            FROM (SELECT bigTbl.key1 as key1,
                         bigTbl.key2 as key2,
                         smallTbl1.key as key3,
                         bigTbl.value as value1,
                         bigTbl.value as value2
                  FROM bigTbl
                  JOIN smallTbl1 ON (bigTbl.key1 = smallTbl1.key)) join1
            JOIN smallTbl2 ON (join1.value1 = smallTbl2.value)) join2
      JOIN smallTbl3 ON (join2.key2 = smallTbl3.key)) join3
JOIN smallTbl4 ON (join3.key3 = smallTbl4.key);

SELECT SUM(HASH(join3.key1)),
       SUM(HASH(join3.key2)),
       SUM(HASH(join3.key3)),
       SUM(HASH(join3.key4)),
       SUM(HASH(join3.key5)),
       SUM(HASH(smallTbl4.key)),
       SUM(HASH(join3.value1)),
       SUM(HASH(join3.value2))
FROM (SELECT join2.key1 as key1,
             join2.key2 as key2,
             join2.key3 as key3,
             join2.key4 as key4,
             smallTbl3.key as key5,
             join2.value1 as value1,
             join2.value2 as value2
      FROM (SELECT join1.key1 as key1,
                   join1.key2 as key2,
                   join1.key3 as key3,
                   smallTbl2.key as key4,
                   join1.value1 as value1,
                   join1.value2 as value2
            FROM (SELECT bigTbl.key1 as key1,
                         bigTbl.key2 as key2,
                         smallTbl1.key as key3,
                         bigTbl.value as value1,
                         bigTbl.value as value2
                  FROM bigTbl
                  JOIN smallTbl1 ON (bigTbl.key1 = smallTbl1.key)) join1
            JOIN smallTbl2 ON (join1.value1 = smallTbl2.value)) join2
      JOIN smallTbl3 ON (join2.key2 = smallTbl3.key)) join3
JOIN smallTbl4 ON (join3.key3 = smallTbl4.key);

set hive.auto.convert.join.noconditionaltask.size=0;
-- Enable noconditionaltask and but set the size of hive.auto.convert.join.noconditionaltask.size
-- to 0. The plan will be the same as the one with a disabled nonconditionaltask.
EXPLAIN
SELECT SUM(HASH(join3.key1)),
       SUM(HASH(join3.key2)),
       SUM(HASH(join3.key3)),
       SUM(HASH(join3.key4)),
       SUM(HASH(join3.key5)),
       SUM(HASH(smallTbl4.key)),
       SUM(HASH(join3.value1)),
       SUM(HASH(join3.value2))
FROM (SELECT join2.key1 as key1,
             join2.key2 as key2,
             join2.key3 as key3,
             join2.key4 as key4,
             smallTbl3.key as key5,
             join2.value1 as value1,
             join2.value2 as value2
      FROM (SELECT join1.key1 as key1,
                   join1.key2 as key2,
                   join1.key3 as key3,
                   smallTbl2.key as key4,
                   join1.value1 as value1,
                   join1.value2 as value2
            FROM (SELECT bigTbl.key1 as key1,
                         bigTbl.key2 as key2,
                         smallTbl1.key as key3,
                         bigTbl.value as value1,
                         bigTbl.value as value2
                  FROM bigTbl
                  JOIN smallTbl1 ON (bigTbl.key1 = smallTbl1.key)) join1
            JOIN smallTbl2 ON (join1.value1 = smallTbl2.value)) join2
      JOIN smallTbl3 ON (join2.key2 = smallTbl3.key)) join3
JOIN smallTbl4 ON (join3.key3 = smallTbl4.key);

SELECT SUM(HASH(join3.key1)),
       SUM(HASH(join3.key2)),
       SUM(HASH(join3.key3)),
       SUM(HASH(join3.key4)),
       SUM(HASH(join3.key5)),
       SUM(HASH(smallTbl4.key)),
       SUM(HASH(join3.value1)),
       SUM(HASH(join3.value2))
FROM (SELECT join2.key1 as key1,
             join2.key2 as key2,
             join2.key3 as key3,
             join2.key4 as key4,
             smallTbl3.key as key5,
             join2.value1 as value1,
             join2.value2 as value2
      FROM (SELECT join1.key1 as key1,
                   join1.key2 as key2,
                   join1.key3 as key3,
                   smallTbl2.key as key4,
                   join1.value1 as value1,
                   join1.value2 as value2
            FROM (SELECT bigTbl.key1 as key1,
                         bigTbl.key2 as key2,
                         smallTbl1.key as key3,
                         bigTbl.value as value1,
                         bigTbl.value as value2
                  FROM bigTbl
                  JOIN smallTbl1 ON (bigTbl.key1 = smallTbl1.key)) join1
            JOIN smallTbl2 ON (join1.value1 = smallTbl2.value)) join2
      JOIN smallTbl3 ON (join2.key2 = smallTbl3.key)) join3
JOIN smallTbl4 ON (join3.key3 = smallTbl4.key);
