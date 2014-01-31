DROP VIEW xxx7;
CREATE VIEW xxx7
PARTITIONED ON (key)
AS 
SELECT hr,key FROM srcpart;

SET hive.mapred.mode=strict;

-- strict mode should cause this to fail since view partition
-- predicate does not correspond to an underlying table partition predicate
ALTER VIEW xxx7 ADD PARTITION (key=10);
