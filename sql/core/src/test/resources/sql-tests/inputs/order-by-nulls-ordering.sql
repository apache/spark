-- Q1. testing window functions with order by
create table spark_10747(col1 int, col2 int, col3 int) using parquet;

-- Q2. insert to tables
INSERT INTO spark_10747 VALUES (6, 12, 10), (6, 11, 4), (6, 9, 10), (6, 15, 8),
(6, 15, 8), (6, 7, 4), (6, 7, 8), (6, 13, null), (6, 10, null);

-- Q3. windowing with order by DESC NULLS LAST
select col1, col2, col3, sum(col2)
    over (partition by col1
       order by col3 desc nulls last, col2
       rows between 2 preceding and 2 following ) as sum_col2
from spark_10747 where col1 = 6 order by sum_col2;

-- Q4. windowing with order by DESC NULLS FIRST
select col1, col2, col3, sum(col2)
    over (partition by col1
       order by col3 desc nulls first, col2
       rows between 2 preceding and 2 following ) as sum_col2
from spark_10747 where col1 = 6 order by sum_col2;

-- Q5. windowing with order by ASC NULLS LAST
select col1, col2, col3, sum(col2)
    over (partition by col1
       order by col3 asc nulls last, col2
       rows between 2 preceding and 2 following ) as sum_col2
from spark_10747 where col1 = 6 order by sum_col2;

-- Q6. windowing with order by ASC NULLS FIRST
select col1, col2, col3, sum(col2)
    over (partition by col1
       order by col3 asc nulls first, col2
       rows between 2 preceding and 2 following ) as sum_col2
from spark_10747 where col1 = 6 order by sum_col2;

-- Q7. Regular query with ORDER BY ASC NULLS FIRST
SELECT COL1, COL2, COL3 FROM spark_10747 ORDER BY COL3 ASC NULLS FIRST, COL2;

-- Q8. Regular query with ORDER BY ASC NULLS LAST
SELECT COL1, COL2, COL3 FROM spark_10747 ORDER BY COL3 NULLS LAST, COL2;

-- Q9. Regular query with ORDER BY DESC NULLS FIRST
SELECT COL1, COL2, COL3 FROM spark_10747 ORDER BY COL3 DESC NULLS FIRST, COL2;

-- Q10. Regular query with ORDER BY DESC NULLS LAST
SELECT COL1, COL2, COL3 FROM spark_10747 ORDER BY COL3 DESC NULLS LAST, COL2;

-- drop the test table
drop table spark_10747;

-- Q11. mix datatype for ORDER BY NULLS FIRST|LAST
create table spark_10747_mix(
col1 string,
col2 int,
col3 double,
col4 decimal(10,2),
col5 decimal(20,1))
using parquet;

-- Q12. Insert to the table
INSERT INTO spark_10747_mix VALUES
('b', 2, 1.0, 1.00, 10.0),
('d', 3, 2.0, 3.00, 0.0),
('c', 3, 2.0, 2.00, 15.1),
('d', 3, 0.0, 3.00, 1.0),
(null, 3, 0.0, 3.00, 1.0),
('d', 3, null, 4.00, 1.0),
('a', 1, 1.0, 1.00, null),
('c', 3, 2.0, 2.00, null);

-- Q13. Regular query with 2 NULLS LAST columns
select * from spark_10747_mix order by col1 nulls last, col5 nulls last;

-- Q14. Regular query with 2 NULLS FIRST columns
select * from spark_10747_mix order by col1 desc nulls first, col5 desc nulls first;

-- Q15. Regular query with mixed NULLS FIRST|LAST
select * from spark_10747_mix order by col5 desc nulls first, col3 desc nulls last;

-- drop the test table
drop table spark_10747_mix;


