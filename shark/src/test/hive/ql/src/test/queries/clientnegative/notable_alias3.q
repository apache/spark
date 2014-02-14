CREATE TABLE dest1(key INT, value DOUBLE) STORED AS TEXTFILE;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', src.key, sum(src.value) WHERE src.key < 100 group by key;
