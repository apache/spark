DESCRIBE FUNCTION count;
DESCRIBE FUNCTION EXTENDED count;

EXPLAIN SELECT count(key) FROM src;
SELECT count(key) FROM src;

EXPLAIN SELECT count(DISTINCT key) FROM src;
SELECT count(DISTINCT key) FROM src;

EXPLAIN SELECT count(DISTINCT key, value) FROM src;
SELECT count(DISTINCT key, value) FROM src;

EXPLAIN SELECT count(*) FROM src;
SELECT count(*) FROM src;

EXPLAIN SELECT count(1) FROM src;
SELECT count(1) FROM src;

select count(1) from src where false;
select count(*) from src where false;
