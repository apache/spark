-- Tests that when overwriting a partition in a table after altering the serde properties
-- the partition metadata is updated as well.

CREATE TABLE tst1(key STRING, value STRING) PARTITIONED BY (ds STRING);

DESCRIBE FORMATTED tst1;

INSERT OVERWRITE TABLE tst1 PARTITION (ds = '1') SELECT key, value FROM src;

DESCRIBE FORMATTED tst1 PARTITION (ds = '1');

-- Test altering the serde properties

ALTER TABLE tst1 SET SERDEPROPERTIES ('field.delim' = ',');

DESCRIBE FORMATTED tst1;

INSERT OVERWRITE TABLE tst1 PARTITION (ds = '1') SELECT key, value FROM src;

DESCRIBE FORMATTED tst1 PARTITION (ds = '1');
