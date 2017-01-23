CREATE TABLE t (a STRING, b INT, c STRING, d STRING) USING parquet PARTITIONED BY (c, d);

ALTER TABLE t ADD PARTITION (c='Us', d=1);

DESCRIBE t;

DESC t;

DESC TABLE t;

-- Ignore these because there exist timestamp results, e.g., `Create Table`.
-- DESC EXTENDED t;
-- DESC FORMATTED t;

DESC t PARTITION (c='Us', d=1);

-- Ignore these because there exist timestamp results, e.g., transient_lastDdlTime.
-- DESC EXTENDED t PARTITION (c='Us', d=1);
-- DESC FORMATTED t PARTITION (c='Us', d=1);

-- NoSuchPartitionException: Partition not found in table
DESC t PARTITION (c='Us', d=2);

-- AnalysisException: Partition spec is invalid
DESC t PARTITION (c='Us');

-- ParseException: PARTITION specification is incomplete
DESC t PARTITION (c='Us', d);

-- DROP TEST TABLE
DROP TABLE t;
