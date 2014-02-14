DROP TABLE infertypes;
CREATE TABLE infertypes(ti TINYINT, si SMALLINT, i INT, bi BIGINT, fl FLOAT, db DOUBLE, str STRING);

LOAD DATA LOCAL INPATH '../data/files/infer_const_type.txt' OVERWRITE INTO TABLE infertypes;

SELECT * FROM infertypes;

EXPLAIN SELECT * FROM infertypes WHERE
  ti  = '127' AND
  si  = 32767 AND
  i   = '12345' AND
  bi  = '-12345' AND
  fl  = '0906' AND
  db  = '-307' AND
  str = 1234;

SELECT * FROM infertypes WHERE
  ti  = '127' AND
  si  = 32767 AND
  i   = '12345' AND
  bi  = '-12345' AND
  fl  = '0906' AND
  db  = '-307' AND
  str = 1234;

-- all should return false as all numbers exceeed the largest number 
-- which could be represented by the corresponding type
-- and string_col = long_const should return false
EXPLAIN SELECT * FROM infertypes WHERE
  ti  = '128' OR
  si  = 32768 OR
  i   = '2147483648' OR
  bi  = '9223372036854775808' OR
  fl  = 'float' OR
  db  = 'double';

SELECT * FROM infertypes WHERE
  ti  = '128' OR
  si  = 32768 OR
  i   = '2147483648' OR
  bi  = '9223372036854775808' OR
  fl  = 'float' OR
  db  = 'double';

-- for the query like: int_col = double, should return false 
EXPLAIN SELECT * FROM infertypes WHERE
  ti  = '127.0' OR
  si  = 327.0 OR
  i   = '-100.0';

SELECT * FROM infertypes WHERE
  ti  = '127.0' OR
  si  = 327.0 OR
  i   = '-100.0';

EXPLAIN SELECT * FROM infertypes WHERE
  ti < '127.0' AND
  i > '100.0' AND
  str = 1.57;

SELECT * FROM infertypes WHERE
  ti < '127.0' AND
  i > '100.0' AND
  str = 1.57;

DROP TABLE infertypes;
