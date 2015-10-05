DROP VIEW xxx18;

-- should fail:  partitioning columns out of order
CREATE VIEW xxx18
PARTITIONED ON (value,key)
AS SELECT key+1 as k2,key,value FROM src;
