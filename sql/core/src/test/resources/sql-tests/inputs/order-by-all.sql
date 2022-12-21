create temporary view data as select * from values
  (0, 1),
  (0, 2),
  (1, 3),
  (1, NULL)
  as data(g, i);

-- most basic test with only 1 column
select g from data order by all;

-- two columns
select * from data order by all;

-- test case insensitive
select * from data order by aLl;

-- asc/desc
select * from data order by all asc;
select * from data order by all desc;

-- nulls first / last
select * from data order by all nulls first;
select * from data order by all nulls last;

-- combining nulls first/last/asc/desc
select * from data order by all asc nulls first;
select * from data order by all desc nulls first;
select * from data order by all asc nulls last;
select * from data order by all desc nulls last;

-- set operations from duckdb
select * from data union all select * from data order by all;
select * from data union select * from data order by all;

-- limit
select * from data order by all limit 2;

-- precedence: if there's a column already named all, reference that, instead of expanding.
-- result should be 1, 2, 3, and not 3, 2, 1
select * from values("z", 1), ("y", 2), ("x", 3) AS T(col1, all) order by all;

-- shouldn't work in window functions
select name, dept, rank() over (partition by dept order by all) as rank
from values('Lisa', 'Sales', 10000, 35) as T(name, dept, salary, age);
