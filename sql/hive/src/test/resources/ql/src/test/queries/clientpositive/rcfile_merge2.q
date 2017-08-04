set hive.merge.rcfile.block.level=true;
set hive.exec.dynamic.partition=true;
set mapreduce.input.fileinputformat.split.maxsize=100;
set mapreduce.input.fileinputformat.split.minsize=1;

DROP TABLE rcfile_merge2a;

CREATE TABLE rcfile_merge2a (key INT, value STRING)
    PARTITIONED BY (one string, two string, three string)
    STORED AS RCFILE;

EXPLAIN INSERT OVERWRITE TABLE rcfile_merge2a PARTITION (one='1', two, three)
    SELECT key, value, PMOD(HASH(key), 10) as two, 
        PMOD(HASH(value), 10) as three
    FROM src;
INSERT OVERWRITE TABLE rcfile_merge2a PARTITION (one='1', two, three)
    SELECT key, value, PMOD(HASH(key), 10) as two, 
        PMOD(HASH(value), 10) as three
    FROM src;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM rcfile_merge2a
) t;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value, '1', PMOD(HASH(key), 10), 
        PMOD(HASH(value), 10)) USING 'tr \t _' AS (c)
    FROM src
) t;

DROP TABLE rcfile_merge2a;

