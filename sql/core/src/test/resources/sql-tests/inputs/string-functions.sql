-- Argument number exception
select concat_ws();
select format_string();

-- A pipe operator for string concatenation
SELECT 'a' || 'b';

-- Check if catalyst collapses multiple `Concat`s
EXPLAIN EXTENDED SELECT (col1 || col2 || col3 || col4) col
FROM (SELECT id col1, id col2, id col3, id col4 FROM range(10));
