DESCRIBE FUNCTION find_in_set;
DESCRIBE FUNCTION EXTENDED find_in_set; 

EXPLAIN
FROM src1 SELECT find_in_set(src1.key,concat(src1.key,',',src1.value));

FROM src1 SELECT find_in_set(src1.key,concat(src1.key,',',src1.value));

SELECT find_in_set('ab','ab,abc,abcde') FROM src1 LIMIT 1;
SELECT find_in_set('ab','abc,ab,bbb') FROM src1 LIMIT 1;
SELECT find_in_set('ab','def,abc,ab') FROM src1 LIMIT 1;
SELECT find_in_set('ab','abc,abd,abf') FROM src1 LIMIT 1;
SELECT find_in_set(null,'a,b,c') FROM src1 LIMIT 1;
SELECT find_in_set('a',null) FROM src1 LIMIT 1;
SELECT find_in_set('', '') FROM src1 LIMIT 1;
SELECT find_in_set('',',') FROM src1 LIMIT 1;
SELECT find_in_set('','a,,b') FROM src1 LIMIT 1;
SELECT find_in_set('','a,b,') FROM src1 LIMIT 1;
SELECT find_in_set(',','a,b,d,') FROM src1 LIMIT 1;
SELECT find_in_set('a','') FROM src1 LIMIT 1;
SELECT find_in_set('a,','a,b,c,d') FROM src1 LIMIT 1;

SELECT * FROM src1 WHERE NOT find_in_set(key,'311,128,345,2,956')=0;
