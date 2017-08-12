set hive.merge.rcfile.block.level=false;
set hive.exec.dynamic.partition=true;
set mapreduce.input.fileinputformat.split.maxsize=100;
set mapref.min.split.size=1;

DROP TABLE rcfile_merge1;
DROP TABLE rcfile_merge1b;

CREATE TABLE rcfile_merge1 (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS RCFILE;
CREATE TABLE rcfile_merge1b (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS RCFILE;

-- Use non block-level merge
EXPLAIN
    INSERT OVERWRITE TABLE rcfile_merge1 PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 100) as part
        FROM src;
INSERT OVERWRITE TABLE rcfile_merge1 PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 100) as part
    FROM src;

set hive.merge.rcfile.block.level=true;
EXPLAIN
    INSERT OVERWRITE TABLE rcfile_merge1b PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 100) as part
        FROM src;
INSERT OVERWRITE TABLE rcfile_merge1b PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 100) as part
    FROM src;

-- Verify
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM rcfile_merge1 WHERE ds='1'
) t;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM rcfile_merge1b WHERE ds='1'
) t;

DROP TABLE rcfile_merge1;
DROP TABLE rcfile_merge1b;
