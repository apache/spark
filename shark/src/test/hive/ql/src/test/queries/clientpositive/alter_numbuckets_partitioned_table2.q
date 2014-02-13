-- Tests that when overwriting a partition in a table after altering the bucketing/sorting metadata
-- the partition metadata is updated as well.

CREATE TABLE tst1(key STRING, value STRING) PARTITIONED BY (ds STRING);

DESCRIBE FORMATTED tst1;

SET hive.enforce.bucketing=true;
SET hive.enforce.sorting=true;
INSERT OVERWRITE TABLE tst1 PARTITION (ds = '1') SELECT key, value FROM src;

DESCRIBE FORMATTED tst1 PARTITION (ds = '1');

-- Test an unbucketed partition gets converted to bucketed
ALTER TABLE tst1 CLUSTERED BY (key) INTO 8 BUCKETS;

DESCRIBE FORMATTED tst1;

INSERT OVERWRITE TABLE tst1 PARTITION (ds = '1') SELECT key, value FROM src;

DESCRIBE FORMATTED tst1 PARTITION (ds = '1');

-- Test an unsorted partition gets converted to sorted
ALTER TABLE tst1 CLUSTERED BY (key) SORTED BY (key DESC) INTO 8 BUCKETS;

DESCRIBE FORMATTED tst1;

INSERT OVERWRITE TABLE tst1 PARTITION (ds = '1') SELECT key, value FROM src;

DESCRIBE FORMATTED tst1 PARTITION (ds = '1');

-- Test changing the bucket columns
ALTER TABLE tst1 CLUSTERED BY (value) SORTED BY (key DESC) INTO 8 BUCKETS;

DESCRIBE FORMATTED tst1;

INSERT OVERWRITE TABLE tst1 PARTITION (ds = '1') SELECT key, value FROM src;

DESCRIBE FORMATTED tst1 PARTITION (ds = '1');

-- Test changing the number of buckets
ALTER TABLE tst1 CLUSTERED BY (value) SORTED BY (key DESC) INTO 4 BUCKETS;

DESCRIBE FORMATTED tst1;

INSERT OVERWRITE TABLE tst1 PARTITION (ds = '1') SELECT key, value FROM src;

DESCRIBE FORMATTED tst1 PARTITION (ds = '1');

-- Test changing the sort columns
ALTER TABLE tst1 CLUSTERED BY (value) SORTED BY (value DESC) INTO 4 BUCKETS;

DESCRIBE FORMATTED tst1;

INSERT OVERWRITE TABLE tst1 PARTITION (ds = '1') SELECT key, value FROM src;

DESCRIBE FORMATTED tst1 PARTITION (ds = '1');

-- Test changing the sort order
ALTER TABLE tst1 CLUSTERED BY (value) SORTED BY (value ASC) INTO 4 BUCKETS;

DESCRIBE FORMATTED tst1;

INSERT OVERWRITE TABLE tst1 PARTITION (ds = '1') SELECT key, value FROM src;

DESCRIBE FORMATTED tst1 PARTITION (ds = '1');

-- Test a sorted partition gets converted to unsorted
ALTER TABLE tst1 CLUSTERED BY (value) INTO 4 BUCKETS;

DESCRIBE FORMATTED tst1;

INSERT OVERWRITE TABLE tst1 PARTITION (ds = '1') SELECT key, value FROM src;

DESCRIBE FORMATTED tst1 PARTITION (ds = '1');

-- Test a bucketed partition gets converted to unbucketed
ALTER TABLE tst1 NOT CLUSTERED;

DESCRIBE FORMATTED tst1;

INSERT OVERWRITE TABLE tst1 PARTITION (ds = '1') SELECT key, value FROM src;

DESCRIBE FORMATTED tst1 PARTITION (ds = '1');
