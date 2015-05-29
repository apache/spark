DROP VIEW xxx17;

-- should fail:  partitioning key must be at end
CREATE VIEW xxx17
PARTITIONED ON (key)
AS SELECT key,value FROM src;
