DROP TABLE parquet_create_staging;
DROP TABLE parquet_create;

CREATE TABLE parquet_create_staging (
    id int,
    str string,
    mp  MAP<STRING,STRING>,
    lst ARRAY<STRING>,
    strct STRUCT<A:STRING,B:STRING>
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

CREATE TABLE parquet_create (
    id int,
    str string,
    mp  MAP<STRING,STRING>,
    lst ARRAY<STRING>,
    strct STRUCT<A:STRING,B:STRING>
) STORED AS PARQUET;

DESCRIBE FORMATTED parquet_create;

LOAD DATA LOCAL INPATH '../../data/files/parquet_create.txt' OVERWRITE INTO TABLE parquet_create_staging;

SELECT * FROM parquet_create_staging;

INSERT OVERWRITE TABLE parquet_create SELECT * FROM parquet_create_staging;

SELECT * FROM parquet_create group by id;
SELECT id, count(0) FROM parquet_create group by id;
SELECT str from parquet_create;
SELECT mp from parquet_create;
SELECT lst from parquet_create;
SELECT strct from parquet_create;
