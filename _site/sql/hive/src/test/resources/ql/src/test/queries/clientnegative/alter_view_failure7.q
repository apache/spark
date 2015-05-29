DROP VIEW xxx8;
CREATE VIEW xxx8
PARTITIONED ON (ds,hr)
AS 
SELECT key,ds,hr FROM srcpart;

-- should fail:  need to fill in all partition columns
ALTER VIEW xxx8 ADD PARTITION (ds='2011-01-01');
