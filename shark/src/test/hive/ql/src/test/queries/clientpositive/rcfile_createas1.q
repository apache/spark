set hive.merge.rcfile.block.level=true;
set mapred.max.split.size=100;
set mapred.min.split.size=1;

DROP TABLE rcfile_createas1a;
DROP TABLE rcfile_createas1b;

CREATE TABLE rcfile_createas1a (key INT, value STRING)
    PARTITIONED BY (ds string);
INSERT OVERWRITE TABLE rcfile_createas1a PARTITION (ds='1')
    SELECT * FROM src;
INSERT OVERWRITE TABLE rcfile_createas1a PARTITION (ds='2')
    SELECT * FROM src;

EXPLAIN
    CREATE TABLE rcfile_createas1b
    STORED AS RCFILE AS 
        SELECT key, value, PMOD(HASH(key), 50) as part
        FROM rcfile_createas1a;
CREATE TABLE rcfile_createas1b
    STORED AS RCFILE AS 
        SELECT key, value, PMOD(HASH(key), 50) as part
        FROM rcfile_createas1a;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM rcfile_createas1a
) t;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM rcfile_createas1b
) t;

DROP TABLE rcfile_createas1a;
DROP TABLE rcfile_createas1b;
