set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION split;
DESCRIBE FUNCTION EXTENDED split;

EXPLAIN SELECT 
  split('a b c', ' '),
  split('oneAtwoBthreeC', '[ABC]'),
  split('', '.'),
  split(50401020, 0)
FROM src tablesample (1 rows);

SELECT 
  split('a b c', ' '),
  split('oneAtwoBthreeC', '[ABC]'),
  split('', '.'),
  split(50401020, 0)
FROM src tablesample (1 rows);
