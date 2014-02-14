DROP VIEW xxx6;
CREATE VIEW xxx6
PARTITIONED ON (value)
AS 
SELECT * FROM src;

-- should fail:  partition column name does not match
ALTER VIEW xxx6 ADD PARTITION (v='val_86');
