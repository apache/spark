DROP VIEW xxx4;
CREATE VIEW xxx4 
AS 
SELECT * FROM src;

-- should fail:  need to use ALTER VIEW, not ALTER TABLE
ALTER TABLE xxx4 RENAME TO xxx4a;
