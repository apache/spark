SET hive.vectorized.execution.enabled = true;

-- Test string functions in vectorized mode to verify end-to-end functionality.

explain 
select 
   substr(cstring1, 1, 2)
  ,substr(cstring1, 2)
  ,lower(cstring1)
  ,upper(cstring1)
  ,ucase(cstring1)
  ,length(cstring1)
  ,trim(cstring1)
  ,ltrim(cstring1)
  ,rtrim(cstring1)
  ,concat(cstring1, cstring2)
  ,concat('>', cstring1)
  ,concat(cstring1, '<')
  ,concat(substr(cstring1, 1, 2), substr(cstring2, 1, 2))
from alltypesorc
-- Limit the number of rows of output to a reasonable amount.
where cbigint % 237 = 0
-- Test function use in the WHERE clause.
and length(substr(cstring1, 1, 2)) <= 2
and cstring1 like '%';
 
select 
   substr(cstring1, 1, 2)
  ,substr(cstring1, 2)
  ,lower(cstring1)
  ,upper(cstring1)
  ,ucase(cstring1)
  ,length(cstring1)
  ,trim(cstring1)
  ,ltrim(cstring1)
  ,rtrim(cstring1)
  ,concat(cstring1, cstring2)
  ,concat('>', cstring1)
  ,concat(cstring1, '<')
  ,concat(substr(cstring1, 1, 2), substr(cstring2, 1, 2))
from alltypesorc
-- Limit the number of rows of output to a reasonable amount.
where cbigint % 237 = 0
-- Test function use in the WHERE clause.
and length(substr(cstring1, 1, 2)) <= 2
and cstring1 like '%';
