DESCRIBE FUNCTION split;
DESCRIBE FUNCTION EXTENDED split;

EXPLAIN SELECT 
  split('a b c', ' '),
  split('oneAtwoBthreeC', '[ABC]'),
  split('', '.'),
  split(50401020, 0)
FROM src LIMIT 1;

SELECT 
  split('a b c', ' '),
  split('oneAtwoBthreeC', '[ABC]'),
  split('', '.'),
  split(50401020, 0)
FROM src LIMIT 1;
