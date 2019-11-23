set hive.ddl.output.format=json;

DROP VIEW view_partitioned;

CREATE VIEW view_partitioned
PARTITIONED ON (value)
AS
SELECT key, value
FROM src
WHERE key=86;

ALTER VIEW view_partitioned
ADD PARTITION (value='val_86');

DESCRIBE FORMATTED view_partitioned PARTITION (value='val_86');

DROP VIEW view_partitioned;
