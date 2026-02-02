CREATE TABLE dest1(`location` INT, `type` STRING) PARTITIONED BY(`table` STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 PARTITION(`table`='2008-04-08') SELECT src.key as `partition`, src.value as `from` WHERE src.key >= 200 and src.key < 300;

EXPLAIN
SELECT `int`.`location`, `int`.`type`, `int`.`table` FROM dest1 `int` WHERE `int`.`table` = '2008-04-08';

FROM src
INSERT OVERWRITE TABLE dest1 PARTITION(`table`='2008-04-08') SELECT src.key as `partition`, src.value as `from` WHERE src.key >= 200 and src.key < 300;

SELECT `int`.`location`, `int`.`type`, `int`.`table` FROM dest1 `int` WHERE `int`.`table` = '2008-04-08';
