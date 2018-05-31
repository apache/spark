-- test various operator for comparison, including =, <=>, <, <=, >, >=

create temporary view data as select * from values
  (1, 1.0D, 'a'),
  (2, 2.0D, 'b'),
  (3, 3.0D, 'c'),
  (null, null, null)
  as data(i, j, k);

-- binary type
select x'00' < x'0f';
select x'00' < x'ff';

-- int type
select i, i = 2, i = null, i <=> 2, i <=> null, i < 2, i <= 2, i > 2, i >= 2 from data;

-- decimal type
select j, j = 2.0D, j = null, j <=> 2.0D, j <=> null, j < 2.0D, j <= 2.0D, j > 2.0D, j >= 2.0D from data;

-- string type
select k, k = 'b', k = null, k <=> 'b', k <=> null, k < 'b', k <= 'b', k > 'b', k >= 'b' from data;

-- struct type
select i, j, (i, j) = (2, 2.0D), (i, j) = null, (i, j) < (2, 3.0D) from data;

-- implicit type cast
select i, j, i = 2L, (i, j) = (2L, 2.0D) from data;
