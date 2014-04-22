CREATE TABLE T1(a STRING, b STRING, c STRING); 

-- Expression 'a' is not in GROUP BY clause
SELECT a FROM T1 GROUP BY b GROUPING SETS (b);
