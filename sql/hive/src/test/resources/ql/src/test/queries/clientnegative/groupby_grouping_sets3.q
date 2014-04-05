CREATE TABLE T1(a STRING, b STRING, c STRING); 

-- Grouping sets expression is not in GROUP BY clause
SELECT a FROM T1 GROUP BY a GROUPING SETS (a, b);
