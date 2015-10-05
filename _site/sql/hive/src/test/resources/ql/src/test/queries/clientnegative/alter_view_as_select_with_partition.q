CREATE VIEW testViewPart PARTITIONED ON (value)
AS
SELECT key, value
FROM src
WHERE key=86;

ALTER VIEW testViewPart 
ADD PARTITION (value='val_86') PARTITION (value='val_xyz'); 
DESCRIBE FORMATTED testViewPart;

-- If a view has partition, could not replace it with ALTER VIEW AS SELECT
ALTER VIEW testViewPart as SELECT * FROM srcpart;
