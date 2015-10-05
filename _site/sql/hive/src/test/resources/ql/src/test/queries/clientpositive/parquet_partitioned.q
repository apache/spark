set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

DROP TABLE parquet_partitioned_staging;
DROP TABLE parquet_partitioned;

CREATE TABLE parquet_partitioned_staging (
    id int,
    str string,
    part string
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

CREATE TABLE parquet_partitioned (
    id int,
    str string
) PARTITIONED BY (part string)
STORED AS PARQUET;

DESCRIBE FORMATTED parquet_partitioned;

LOAD DATA LOCAL INPATH '../../data/files/parquet_partitioned.txt' OVERWRITE INTO TABLE parquet_partitioned_staging;

SELECT * FROM parquet_partitioned_staging;

INSERT OVERWRITE TABLE parquet_partitioned PARTITION (part) SELECT * FROM parquet_partitioned_staging;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT * FROM parquet_partitioned ORDER BY id, str;
SELECT part, COUNT(0) FROM parquet_partitioned GROUP BY part;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SELECT * FROM parquet_partitioned ORDER BY id, str;
SELECT part, COUNT(0) FROM parquet_partitioned GROUP BY part;
