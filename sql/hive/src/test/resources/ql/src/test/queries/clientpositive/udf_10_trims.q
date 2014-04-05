CREATE TABLE dest1(c1 STRING) STORED AS TEXTFILE;

EXPLAIN
INSERT OVERWRITE TABLE dest1
SELECT trim(trim(trim(trim(trim(trim(trim(trim(trim(trim( '  abc  '))))))))))
FROM src
WHERE src.key = 86;

INSERT OVERWRITE TABLE dest1
SELECT trim(trim(trim(trim(trim(trim(trim(trim(trim(trim( '  abc  '))))))))))
FROM src
WHERE src.key = 86;
