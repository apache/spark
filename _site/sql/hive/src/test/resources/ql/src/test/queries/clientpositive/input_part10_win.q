-- INCLUDE_OS_WINDOWS
-- included only on  windows because of difference in file name encoding logic

CREATE TABLE part_special (
  a STRING,
  b STRING
) PARTITIONED BY (
  ds STRING,
  ts STRING
);

EXPLAIN
INSERT OVERWRITE TABLE part_special PARTITION(ds='2008 04 08', ts = '10:11:12=455')
SELECT 1, 2 FROM src LIMIT 1;

INSERT OVERWRITE TABLE part_special PARTITION(ds='2008 04 08', ts = '10:11:12=455')
SELECT 1, 2 FROM src LIMIT 1;

DESCRIBE EXTENDED part_special PARTITION(ds='2008 04 08', ts = '10:11:12=455');

SELECT * FROM part_special WHERE ds='2008 04 08' AND ts = '10:11:12=455';


