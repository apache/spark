DROP VIEW xxx4;
CREATE VIEW xxx4 
PARTITIONED ON (value)
AS 
SELECT * FROM src;

-- should fail:  need to use ALTER VIEW, not ALTER TABLE
ALTER TABLE xxx4 ADD PARTITION (value='val_86');
