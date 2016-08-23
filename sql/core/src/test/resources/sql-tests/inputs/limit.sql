
-- limit on various data types
select * from testdata limit 2;
select * from arraydata limit 2;
select * from mapdata limit 2;

-- foldable non-literal in limit
select * from testdata limit 2 + 1;

select * from testdata limit CAST(1 AS int);

-- limit must be non-negative
select * from testdata limit -1;

-- limit must be foldable
select * from testdata limit key > 3;

-- limit must be integer
select * from testdata limit true;
select * from testdata limit 'a';

-- limit within a subquery
select * from (select * from range(10) limit 5) where id > 3;
