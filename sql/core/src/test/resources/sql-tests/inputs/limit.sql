
-- limit on various data types
select * from uniqueRowData limit 2;
select * from arraydata limit 2;
select * from mapdata limit 2;

-- foldable non-literal in limit
select * from uniqueRowData limit 2 + 1;

select * from uniqueRowData limit CAST(1 AS int);

-- limit must be non-negative
select * from uniqueRowData limit -1;

-- limit must be foldable
select * from uniqueRowData limit key > 3;

-- limit must be integer
select * from uniqueRowData limit true;
select * from uniqueRowData limit 'a';

-- limit within a subquery
select * from (select * from range(10) limit 5) where id > 3;
