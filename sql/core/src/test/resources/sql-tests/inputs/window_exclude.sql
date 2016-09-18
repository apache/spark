-- Q1. testing window function with exclude clause
create table table1 (col1 int, col2 int, col3 int) using parquet;

insert into table1 values(6, 12, 10), (6, 11, 4), (6, 13, 11), 
(6, 9, 10), (6, 15, 8), (6, 10, 1), (6, 15, 8), (6, 7, 4), (6, 7, 8);

-- sliding frame with exclude current row
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between 2 preceding and 2 following exclude current row)
from table1 where col1 = 6 order by col3;

-- sliding frame with exclude group
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between 2 preceding and 2 following exclude group) 
from table1 where col1 = 6 order by col3;

-- sliding frame with exclude ties
select col1, col2, col3, sum(col2)
    over (partition by col1 order by col3, col2 rows 
    between 2 preceding and 2 following exclude ties)
from table1 where col1 = 6 order by col3;

-- sliding frame with exclude no others
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between 2 preceding and 2 following)
from table1 where col1 = 6 order by col3;

-- expanding frame with exclude current row
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between unbounded preceding and current row exclude current row)
from table1 where col1 = 6 order by col3;

-- expanding frame with exclude group
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between unbounded preceding and current row exclude group)
from table1 where col1 = 6 order by col3;

-- expanding frame with exclude ties
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between unbounded preceding and current row exclude ties) 
from table1 where col1 = 6 order by col3;

-- expanding frame with exclude no others
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between unbounded preceding and current row) 
from table1 where col1 = 6 order by col3;

-- shrinking frame with exclude current row
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between current row and unbounded following exclude current row)
from table1 where col1 = 6 order by col3;

-- shrinking frame with exclude current group
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between current row and unbounded following exclude group)
from table1 where col1 = 6 order by col3;

-- shrinking frame with exclude current ties
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between current row and unbounded following exclude ties)
from table1 where col1 = 6 order by col3;

-- shrinking frame with exclude current no others
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between current row and unbounded following) 
from table1 where col1 = 6 order by col3;

-- whole partition frame with exclude current row
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between unbounded preceding and unbounded following exclude current row)
from table1 where col1 = 6 order by col3;

-- whole partition frame with exclude group
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between unbounded preceding and unbounded following exclude group)
from table1 where col1 = 6 order by col3;

-- whole partition frame with exclude ties
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between unbounded preceding and  unbounded following exclude ties) 
from table1 where col1 = 6 order by col3;

-- whole partition frame with exclude no others
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3, col2 rows 
    between unbounded preceding and unbounded following) 
from table1 where col1 = 6 order by col3;

-- sliding range framing
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between 3 preceding and 3 following)
from table1 where col1 = 6 order by col3;

-- sliding range framing with exclude current row
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between 3 preceding and 3 following exclude current row)
from table1 where col1 = 6 order by col3;

-- sliding range framing with exclude group
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between 3 preceding and 3 following exclude group )
from table1 where col1 = 6 order by col3;

-- sliding range framing with exclude ties
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between 3 preceding and 3 following exclude ties)
from table1 where col1 = 6 order by col3;

-- expanding range framing
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between unbounded preceding and current row)
from table1 where col1 = 6 order by col3;

-- expanding range framing with exclude current row
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between unbounded preceding and current row exclude current row)
from table1 where col1 = 6 order by col3;

-- expanding range framing with exclude group
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between unbounded preceding and current row exclude group)
from table1 where col1 = 6 order by col3;

-- expanding range framing with exclude ties
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between unbounded preceding and current row exclude ties) 
from table1 where col1 = 6 order by col3;

-- shrinking range framing
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between current row and unbounded following ) 
from table1 where col1 = 6 order by col3;

-- shrinking range framing with exclude current row
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between current row and unbounded following exclude current row)
from table1 where col1 = 6 order by col3;

-- shrinking range framing with exclude group
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between current row and unbounded following exclude group)
from table1 where col1 = 6 order by col3;

-- shrinking range framing with exclude ties
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between current row and unbounded following exclude ties)
from table1 where col1 = 6 order by col3;

-- whole partition range framing
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between unbounded preceding and unbounded following )
from table1 where col1 = 6 order by col3;

-- whole partition range framing with exclude current row
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between unbounded preceding and unbounded following exclude current row) 
from table1 where col1 = 6 order by col3;

-- whole partition range framing with exclude group
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between unbounded preceding and unbounded following exclude group) 
from table1 where col1 = 6 order by col3;

-- whole partition range framing with exclude ties
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range 
    between unbounded preceding and unbounded following exclude ties) 
from table1 where col1 = 6 order by col3;
  

-- order by with component expression
select col1, col2, col3, sum(col2) 
    over (partition by col1 order by col3 range
    between unbounded preceding and current row exclude current row)
from table1 where col1 = 6 order by col3;
 

-- trying on other aggregation functions, like AVG
select col1, col2, col3, AVG(col2) 
    over (partition by col1 order by col3 range 
    between unbounded preceding and current row exclude current row) 
from table1 where col1 = 6 order by col3;
  
-- trying on other aggregation functions, like MIN
select col1, col2, col3, MIN(col2) over 
(partition by col1 order by col3 range between unbounded preceding and 
current row exclude group) 
from table1 where col1 = 6 order by col3;
 
-- trying on other aggregation functions, like MAX
select col1, col2, col3, MAX(col2) over 
(partition by col1 order by col3 range between unbounded preceding and 
current row exclude current row) 
from table1 where col1 = 6 order by col3;
  
-- trying on other aggregation functions, like MAX
select col1, col2, col3, MAX(col2) 
    over (partition by col1 order by col3 range 
    between unbounded preceding and current row exclude group) 
from table1 where col1 = 6 order by col3;

-- Non windowAggregation with exclude clause
select col1, cume_dist()
    over (partition by col1 order by col3 range
    between unbounded preceding and current row exclude current row)
from table1;

select col1, rank()
    over (partition by col1 order by col3 range
    between unbounded preceding and current row exclude current row)
from table1;

select col1, lead(2)
    over (partition by col1 order by col3 range
    between unbounded preceding and  current row exclude current row)
from table1;

-- more than one partition
create table table2 (col1 int, col2 int, col3 int) using parquet;

insert into table2 values (6, 12, 10), (6, 11, 4), (6, 13, 11), (6, 9, 10), (6, 15, 8),
(6, 10, 1), (6, 15, 8), (6, 7, 4), (6, 7, 8), (7, 12, 10), (7, 11, 4),
(7, 13, 11), (7, 9, 10), (7, 15, 8), (7, 10, 1), (7, 15, 8), (7, 7, 4), (7, 7, 8);

select col1, col2, col3, sum(col2)
    over (partition by col1 order by col3 range
    between unbounded preceding and current row exclude current row )
from table2 where col1 < 20 order by col1;

DROP TABLE table1;
DROP TABLE table2;




 




