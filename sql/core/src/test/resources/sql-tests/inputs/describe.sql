CREATE TABLE t (a STRING, b INT) PARTITIONED BY (c STRING, d STRING);

ALTER TABLE t ADD PARTITION (c='Us', d=1);

DESC t;

-- Ignore these because there exists timestamp results, e.g., `Create Table`
-- DESC EXTENDED t;
-- DESC FORMATTED t;

DESC t PARTITION (c='Us', d=1);

DESC EXTENDED t PARTITION (c='Us', d=1);

DESC FORMATTED t PARTITION (c='Us', d=1);

-- NoSuchPartitionException: Partition not found in table
DESC t PARTITION (c='Us', d=2);

-- AnalysisException: Partition spec is invalid
DESC t PARTITION (c='Us');

-- ParseException: Unsupported SQL statement
DESC t PARTITION (c='Us', d);
