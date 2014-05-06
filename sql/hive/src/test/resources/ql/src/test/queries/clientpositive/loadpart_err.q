set hive.cli.errors.ignore=true;

ADD FILE ../data/scripts/error_script;

-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.17, 0.18, 0.19, 0.20, 0.20S, 0.23)
-- (this test is flaky so it is currently disabled for all Hadoop versions)

CREATE TABLE loadpart1(a STRING, b STRING) PARTITIONED BY (ds STRING);

INSERT OVERWRITE TABLE loadpart1 PARTITION (ds='2009-01-01')
SELECT TRANSFORM(src.key, src.value) USING 'error_script' AS (tkey, tvalue)
FROM src;

DESCRIBE loadpart1;
SHOW PARTITIONS loadpart1;

LOAD DATA LOCAL INPATH '../data1/files/kv1.txt' INTO TABLE loadpart1 PARTITION(ds='2009-05-05');
SHOW PARTITIONS loadpart1;


