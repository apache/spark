
DROP TABLE IF EXISTS table_complex_type;

CREATE TABLE table_complex_type (
       a STRING,
       b ARRAY<STRING>,
       c ARRAY<MAP<STRING,STRING>>,
       d MAP<STRING,ARRAY<STRING>>
       ) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/create_nested_type.txt' OVERWRITE INTO TABLE table_complex_type;


explain 
analyze table table_complex_type compute statistics for columns d;

analyze table table_complex_type  compute statistics for columns d;
