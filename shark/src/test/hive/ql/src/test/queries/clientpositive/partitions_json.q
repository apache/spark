set hive.ddl.output.format=json;

CREATE TABLE add_part_test (key STRING, value STRING) PARTITIONED BY (ds STRING);
SHOW PARTITIONS add_part_test;

ALTER TABLE add_part_test ADD PARTITION (ds='2010-01-01');
SHOW PARTITIONS add_part_test;

ALTER TABLE add_part_test ADD IF NOT EXISTS PARTITION (ds='2010-01-01');
SHOW PARTITIONS add_part_test;

ALTER TABLE add_part_test ADD IF NOT EXISTS PARTITION (ds='2010-01-02');
SHOW PARTITIONS add_part_test;

SHOW TABLE EXTENDED LIKE add_part_test PARTITION (ds='2010-01-02');

ALTER TABLE add_part_test DROP PARTITION (ds='2010-01-02');

DROP TABLE add_part_test;

set hive.ddl.output.format=text;
