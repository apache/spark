CREATE TABLE npe_test (key STRING, value STRING) PARTITIONED BY (ds STRING);

INSERT OVERWRITE TABLE npe_test PARTITION(ds='2012-12-11')
SELECT src.key, src.value FROM src WHERE key < '200';

INSERT OVERWRITE TABLE npe_test PARTITION(ds='2012-12-12')
SELECT src.key, src.value FROM src WHERE key > '200';

SELECT count(*) FROM npe_test;

EXPLAIN SELECT * FROM npe_test WHERE ds > 2012-11-31 OR ds < 2012-12-15;

SELECT count(*) FROM npe_test WHERE ds > 2012-11-31 OR ds < 2012-12-15;

DROP TABLE npe_test;
