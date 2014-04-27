CREATE TABLE dest1(key STRING, value STRING) STORED AS TEXTFILE;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT concat('1234', 'abc', 'extra argument'), src.value WHERE src.key < 100;

SELECT dest1.* FROM dest1;


