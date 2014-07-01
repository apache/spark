set hive.merge.rcfile.block.level=true;
set mapred.max.split.size=100;
set mapred.min.split.size=1;

DROP TABLE rcfile_merge3a;
DROP TABLE rcfile_merge3b;

CREATE TABLE rcfile_merge3a (key int, value string) 
    PARTITIONED BY (ds string) STORED AS TEXTFILE;
CREATE TABLE rcfile_merge3b (key int, value string) STORED AS RCFILE;

INSERT OVERWRITE TABLE rcfile_merge3a PARTITION (ds='1')
    SELECT * FROM src;
INSERT OVERWRITE TABLE rcfile_merge3a PARTITION (ds='2')
    SELECT * FROM src;

EXPLAIN INSERT OVERWRITE TABLE rcfile_merge3b
    SELECT key, value FROM rcfile_merge3a;
INSERT OVERWRITE TABLE rcfile_merge3b
    SELECT key, value FROM rcfile_merge3a;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM rcfile_merge3a
) t;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM rcfile_merge3b
) t;

DROP TABLE rcfile_merge3a;
DROP TABLE rcfile_merge3b;
