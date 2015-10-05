DROP VIEW vp1;
DROP VIEW vp2;
DROP VIEW vp3;

-- test partitioned view definition
-- (underlying table is not actually partitioned)
CREATE VIEW vp1
PARTITIONED ON (value)
AS
SELECT key, value
FROM src
WHERE key=86;
DESCRIBE EXTENDED vp1;
DESCRIBE FORMATTED vp1;

SELECT * FROM vp1;

SELECT key FROM vp1;

SELECT value FROM vp1;

ALTER VIEW vp1
ADD PARTITION (value='val_86') PARTITION (value='val_xyz');

-- should work since we use IF NOT EXISTS
ALTER VIEW vp1
ADD IF NOT EXISTS PARTITION (value='val_xyz');

SHOW PARTITIONS vp1;

SHOW PARTITIONS vp1 PARTITION(value='val_86');

SHOW TABLE EXTENDED LIKE vp1;

SHOW TABLE EXTENDED LIKE vp1 PARTITION(value='val_86');

ALTER VIEW vp1
DROP PARTITION (value='val_xyz');

SET hive.exec.drop.ignorenonexistent=false;

-- should work since we use IF EXISTS
ALTER VIEW vp1
DROP IF EXISTS PARTITION (value='val_xyz');

SHOW PARTITIONS vp1;

SET hive.mapred.mode=strict;

-- Even though no partition predicate is specified in the next query,
-- the WHERE clause inside of the view should satisfy strict mode.
-- In other words, strict only applies to underlying tables
-- (regardless of whether or not the view is partitioned).
SELECT * FROM vp1;

SET hive.mapred.mode=nonstrict;

-- test a partitioned view on top of an underlying partitioned table,
-- but with only a suffix of the partitioning columns
CREATE VIEW vp2
PARTITIONED ON (hr)
AS SELECT * FROM srcpart WHERE key < 10;
DESCRIBE FORMATTED vp2;

ALTER VIEW vp2 ADD PARTITION (hr='11') PARTITION (hr='12');
SELECT key FROM vp2 WHERE hr='12' ORDER BY key;

-- test a partitioned view where the PARTITIONED ON clause references
-- an imposed column name
CREATE VIEW vp3(k,v)
PARTITIONED ON (v)
AS
SELECT key, value
FROM src
WHERE key=86;
DESCRIBE FORMATTED vp3;

ALTER VIEW vp3
ADD PARTITION (v='val_86');

DROP VIEW vp1;
DROP VIEW vp2;
DROP VIEW vp3;
