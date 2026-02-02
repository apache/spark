DROP TABLE parquet_types_staging;
DROP TABLE parquet_types;

CREATE TABLE parquet_types_staging (
  cint int,
  ctinyint tinyint,
  csmallint smallint,
  cfloat float,
  cdouble double,
  cstring1 string
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

CREATE TABLE parquet_types (
  cint int,
  ctinyint tinyint,
  csmallint smallint,
  cfloat float,
  cdouble double,
  cstring1 string
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/parquet_types.txt' OVERWRITE INTO TABLE parquet_types_staging;

INSERT OVERWRITE TABLE parquet_types SELECT * FROM parquet_types_staging;

SELECT * FROM parquet_types;

SELECT ctinyint,
  MAX(cint),
  MIN(csmallint),
  COUNT(cstring1),
  AVG(cfloat),
  STDDEV_POP(cdouble)
FROM parquet_types
GROUP BY ctinyint
ORDER BY ctinyint
;
