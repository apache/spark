DROP VIEW xxx15;

-- should fail:  baz is not a column
CREATE VIEW xxx15
PARTITIONED ON (baz)
AS SELECT key FROM src;
