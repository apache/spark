DROP VIEW xxx16;

-- should fail:  must have at least one non-partitioning column
CREATE VIEW xxx16
PARTITIONED ON (key)
AS SELECT key FROM src;
