

CREATE TABLE table1 (
       a STRING,
       b ARRAY<STRING>,
       c ARRAY<MAP<STRING,STRING>>,
       d MAP<STRING,ARRAY<STRING>>
       ) STORED AS TEXTFILE;
DESCRIBE table1;
DESCRIBE EXTENDED table1;

LOAD DATA LOCAL INPATH '../data/files/create_nested_type.txt' OVERWRITE INTO TABLE table1;

SELECT * from table1;


