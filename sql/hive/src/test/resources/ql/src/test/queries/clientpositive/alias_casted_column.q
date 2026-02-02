-- HIVE-2477 Use name of original expression for name of CAST output
explain select key from (select cast(key as int) from src )t;

--backward
explain select key2 from (select cast(key as int) key2 from src )t;
