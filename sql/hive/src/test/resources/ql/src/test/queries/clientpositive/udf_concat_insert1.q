CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', concat(src.key) WHERE src.key < 100 group by src.key;

SELECT dest1.* FROM dest1;

